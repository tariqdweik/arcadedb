package com.arcadedb.database.async;

import com.arcadedb.database.*;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PRawRecordCallback;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.graph.*;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PLogManager;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PDatabaseAsyncExecutor {
  private final PDatabaseInternal database;
  private       AsyncThread[]     executorThreads;
  private int     parallelLevel     = -1;
  private int     commitEvery       = 10000;
  private boolean transactionUseWAL = true;
  private boolean transactionSync   = false;

  // SPECIAL COMMANDS
  private final static PDatabaseAsyncCommand FORCE_COMMIT = new PDatabaseAsyncCommand();
  private final static PDatabaseAsyncCommand FORCE_EXIT   = new PDatabaseAsyncCommand();

  private POkCallback    onOkCallback;
  private PErrorCallback onErrorCallback;

  private class AsyncThread extends Thread {
    public final ArrayBlockingQueue<PDatabaseAsyncCommand> queue = new ArrayBlockingQueue<>(1024);
    public final PDatabaseInternal database;
    public volatile boolean shutdown      = false;
    public volatile boolean forceShutdown = false;
    public          long    count         = 0;

    private AsyncThread(final PDatabaseInternal database, final int id) {
      super("AsyncCreateRecord-" + id);
      this.database = database;
    }

    @Override
    public void run() {
      if (PTransactionTL.INSTANCE.get() == null)
        PTransactionTL.INSTANCE.set(new PTransactionContext(database));

      database.getTransaction().setUseWAL(transactionUseWAL);
      database.getTransaction().setSync(transactionSync);
      database.getTransaction().begin();
      while (!forceShutdown) {
        try {
          final PDatabaseAsyncCommand message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            if (message == FORCE_COMMIT) {
              // COMMIT SPECIAL CASE
              try {
                database.getTransaction().commit();
                onOk();
              } catch (Exception e) {
                onError(e);
              }
              database.getTransaction().begin();

            } else if (message == FORCE_EXIT) {
              break;

            } else if (message instanceof PDatabaseAsyncCreateRecord) {
              final PDatabaseAsyncCreateRecord command = (PDatabaseAsyncCreateRecord) message;

              beginTxIfNeeded();

              try {

                database.createRecordNoLock(command.record, command.bucket.getName());

                if (command.record instanceof PModifiableDocument) {
                  final PModifiableDocument doc = (PModifiableDocument) command.record;
                  database.indexDocument(doc, database.getSchema().getType(doc.getType()), command.bucket);
                }

                count++;

                if (count % commitEvery == 0) {
                  database.getTransaction().commit();
                  onOk();
                  database.getTransaction().begin();
                }
              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof PDatabaseAsyncScanType) {

              final PDatabaseAsyncScanType command = (PDatabaseAsyncScanType) message;

              try {
                command.bucket.scan(new PRawRecordCallback() {
                  @Override
                  public boolean onRecord(final PRID rid, final PBinary view) {
                    if (shutdown)
                      return false;

                    final PRecord record = database.getRecordFactory()
                        .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);

                    return command.userCallback.onRecord((PDocument) record);
                  }
                });
              } finally {
                // UNLOCK THE CALLER THREAD
                command.semaphore.countDown();
              }
            } else if (message instanceof PDatabaseAsyncCreateOutEdge) {

              final PDatabaseAsyncCreateOutEdge command = (PDatabaseAsyncCreateOutEdge) message;

              try {
                beginTxIfNeeded();

                PRID outEdgesHeadChunk = command.sourceVertex.getOutEdgesHeadChunk();

                final PVertexInternal modifiableSourceVertex;
                if (outEdgesHeadChunk == null) {
                  final PModifiableEdgeChunk outChunk = new PModifiableEdgeChunk(database,
                      PGraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(outChunk, PGraphEngine
                      .getEdgesBucketName(database, command.sourceVertex.getIdentity().getBucketId(), PVertex.DIRECTION.OUT));
                  outEdgesHeadChunk = outChunk.getIdentity();

                  modifiableSourceVertex = (PVertexInternal) command.sourceVertex.modify();
                  modifiableSourceVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableSourceVertex);
                } else
                  modifiableSourceVertex = command.sourceVertex;

                final PEdgeLinkedList outLinkedList = new PEdgeLinkedList(modifiableSourceVertex, PVertex.DIRECTION.OUT,
                    (PEdgeChunk) database.lookupByRID(modifiableSourceVertex.getOutEdgesHeadChunk(), true));

                outLinkedList.add(command.edgeRID, command.destinationVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof PDatabaseAsyncCreateInEdge) {

              final PDatabaseAsyncCreateInEdge command = (PDatabaseAsyncCreateInEdge) message;

              try {
                beginTxIfNeeded();

                PRID inEdgesHeadChunk = command.destinationVertex.getInEdgesHeadChunk();

                final PVertexInternal modifiableDestinationVertex;
                if (inEdgesHeadChunk == null) {
                  final PModifiableEdgeChunk inChunk = new PModifiableEdgeChunk(database, PGraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(inChunk, PGraphEngine
                      .getEdgesBucketName(database, command.destinationVertex.getIdentity().getBucketId(), PVertex.DIRECTION.IN));
                  inEdgesHeadChunk = inChunk.getIdentity();

                  modifiableDestinationVertex = (PVertexInternal) command.destinationVertex.modify();
                  modifiableDestinationVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableDestinationVertex);
                } else
                  modifiableDestinationVertex = command.destinationVertex;

                final PEdgeLinkedList inLinkedList = new PEdgeLinkedList(modifiableDestinationVertex, PVertex.DIRECTION.IN,
                    (PEdgeChunk) database.lookupByRID(modifiableDestinationVertex.getInEdgesHeadChunk(), true));

                inLinkedList.add(command.edgeRID, command.sourceVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            }
          } else if (shutdown)
            break;

        } catch (PConcurrentModificationException e) {
          PLogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
          database.getTransaction().begin();

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          queue.clear();
          break;
        } catch (Exception e) {
          PLogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
        }
      }

      try {
        database.getTransaction().commit();
        onOk();
      } catch (Exception e) {
        onError(e);
      }
    }
  }

  private void beginTxIfNeeded() {
    if (!database.getTransaction().isActive())
      database.getTransaction().begin();
  }

  private class DatabaseScanAsyncThread extends Thread {
    public final ArrayBlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(2048);
    public final PDatabaseInternal database;
    public volatile boolean shutdown = false;

    private DatabaseScanAsyncThread(final PDatabaseInternal database, final int id) {
      super("AsyncScan-" + id);
      this.database = database;
    }

    @Override
    public void run() {
      if (PTransactionTL.INSTANCE.get() == null)
        PTransactionTL.INSTANCE.set(new PTransactionContext(database));

      while (!shutdown) {
        try {
          final Object[] message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            if (message[0] == FORCE_EXIT) {
              break;
            } else {
            }
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          queue.clear();
          break;
        } catch (Exception e) {
          PLogManager.instance().error(this, "Error on parallel scan", e);
        }
      }
    }
  }

  public class PDBAsynchStats {
    public long queueSize;
  }

  public PDatabaseAsyncExecutor(final PDatabaseInternal database) {
    this.database = database;
    createThreads(Runtime.getRuntime().availableProcessors());
  }

  public PDBAsynchStats getStats() {
    final PDBAsynchStats stats = new PDBAsynchStats();
    stats.queueSize = 0;

    if (executorThreads != null)
      for (int i = 0; i < executorThreads.length; ++i)
        stats.queueSize += executorThreads[i].queue.size();

    return stats;
  }

  public void setTransactionUseWAL(final boolean transactionUseWAL) {
    this.transactionUseWAL = transactionUseWAL;
    createThreads(parallelLevel);
  }

  public boolean isTransactionUseWAL() {
    return transactionUseWAL;
  }

  public void setTransactionSync(final boolean transactionSync) {
    this.transactionSync = transactionSync;
    createThreads(parallelLevel);
  }

  public boolean isTransactionSync() {
    return transactionSync;
  }

  public void onOk(final POkCallback callback) {
    onOkCallback = callback;
  }

  public void onError(final PErrorCallback callback) {
    onErrorCallback = callback;
  }

  public void waitCompletion() {
    if (executorThreads == null)
      return;

    for (int i = 0; i < executorThreads.length; ++i) {
      try {
        executorThreads[i].queue.put(FORCE_COMMIT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    int completed = 0;
    while (true) {
      for (int i = 0; i < executorThreads.length; ++i) {
        final int messages = executorThreads[i].queue.size();
        if (messages == 0)
          ++completed;
        else {
          PLogManager.instance()
              .debug(this, "Waiting for completion async thread %s found %d messages still to be processed", executorThreads[i],
                  messages);
          break;
        }
      }

      if (completed < executorThreads.length) {
        try {
          Thread.sleep(100);
          continue;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      break;
    }
  }

  public void scanType(final String typeName, final PDocumentCallback callback) {
    try {
      final PDocumentType type = database.getSchema().getType(typeName);

      final List<PBucket> buckets = type.getBuckets();
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (PBucket b : buckets) {
        final int slot = b.getId() % parallelLevel;

        try {
          executorThreads[slot].queue.put(new PDatabaseAsyncScanType(semaphore, callback, b));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new PDatabaseOperationException("Error on executing save");
        }
      }

      semaphore.await();

    } catch (Exception e) {
      throw new PDatabaseOperationException(
          "Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
    }
  }

  public void createRecord(final PModifiableDocument record) {
    final PDocumentType type = database.getSchema().getType(record.getType());

    if (record.getIdentity() == null) {
      // NEW
      final PBucket bucket = type.getBucketToSave();
      final int slot = bucket.getId() % parallelLevel;

      try {
        executorThreads[slot].queue.put(new PDatabaseAsyncCreateRecord(record, bucket));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PDatabaseOperationException("Error on executing save");
      }

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void createRecord(final PRecord record, final String bucketName) {
    final PBucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = bucket.getId() % parallelLevel;

    if (record.getIdentity() == null)
      // NEW
      try {
        executorThreads[slot].queue.put(new PDatabaseAsyncCreateRecord(record, bucket));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PDatabaseOperationException("Error on executing save");
      }
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  /**
   * The current thread executes 2 lookups + create the edge. The creation of the 2 edge branches are delegated to asynchronous operations.
   */
  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue,
      final String destinationVertexType, final String[] destinationVertexKey, final Object[] destinationVertexValue,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {
    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<PRID> v1Result = database.lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);

    PVertexInternal sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = database.newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKey.length; ++i)
          ((PModifiableVertex) sourceVertex).set(sourceVertexKey[i], sourceVertexValue[i]);
      } else
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));
    } else
      sourceVertex = (PVertexInternal) v1Result.next().getRecord();

    final Iterator<PRID> v2Result = database.lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);
    PVertexInternal destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = database.newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKey.length; ++i)
          ((PModifiableVertex) destinationVertex).set(destinationVertexKey[i], destinationVertexValue[i]);
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays
                .toString(destinationVertexValue));
    } else
      destinationVertex = (PVertexInternal) v2Result.next().getRecord();

    newEdge(sourceVertex, edgeType, destinationVertex, bidirectional, properties);
  }

  /**
   * Test onluy API.
   */
  public void kill() {
    if (executorThreads != null) {
      // WAIT FOR SHUTDOWN, MAX 1S EACH
      for (int i = 0; i < executorThreads.length; ++i) {
        executorThreads[i].forceShutdown = true;
        executorThreads[i] = null;
      }
    }
  }

  public void close() {
    shutdownThreads();
  }

  public int getParallelLevel() {
    return parallelLevel;
  }

  public void setParallelLevel(final int parallelLevel) {
    if (parallelLevel != this.parallelLevel)
      createThreads(parallelLevel);
  }

  public int getCommitEvery() {
    return commitEvery;
  }

  public void setCommitEvery(final int commitEvery) {
    this.commitEvery = commitEvery;
  }

  private void createThreads(final int parallelLevel) {
    shutdownThreads();

    executorThreads = new AsyncThread[parallelLevel];
    for (int i = 0; i < parallelLevel; ++i) {
      executorThreads[i] = new AsyncThread(database, i);
      executorThreads[i].start();
    }

    this.parallelLevel = parallelLevel;
  }

  private void shutdownThreads() {
    try {
      if (executorThreads != null) {
        // WAIT FOR SHUTDOWN, MAX 1S EACH
        for (int i = 0; i < executorThreads.length; ++i) {
          executorThreads[i].shutdown = true;
          executorThreads[i].queue.put(FORCE_EXIT);
          executorThreads[i].join(10000);
        }
      }
    } catch (InterruptedException e) {
      // IGNORE IT
      Thread.currentThread().interrupt();
    }
  }

  private void newEdge(PVertexInternal sourceVertex, final String edgeType, PVertexInternal destinationVertex,
      final boolean bidirectional, final Object... properties) {
    if (destinationVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final PRID rid = sourceVertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (destinationVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PDatabaseInternal database = (PDatabaseInternal) sourceVertex.getDatabase();

    try {
      final PModifiableEdge edge = new PModifiableEdge(database, edgeType, rid, destinationVertex.getIdentity());
      PGraphEngine.setProperties(edge, properties);
      edge.save();

      try {
        executorThreads[rid.getBucketId() % parallelLevel].queue
            .put(new PDatabaseAsyncCreateOutEdge(sourceVertex, edge.getIdentity(), destinationVertex.getIdentity()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PDatabaseOperationException("Error on creating edge link from out to in");
      }

      if (bidirectional)
        try {
          executorThreads[destinationVertex.getIdentity().getBucketId() % parallelLevel].queue
              .put(new PDatabaseAsyncCreateInEdge(destinationVertex, edge.getIdentity(), sourceVertex.getIdentity()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new PDatabaseOperationException("Error on creating edge link from out to in");
        }

    } catch (Exception e) {
      throw new PDatabaseOperationException("Error on creating edge", e);
    }
  }

  private void onOk() {
    if (onOkCallback != null) {
      try {
        onOkCallback.call();
      } catch (Exception e) {
        PLogManager.instance().error(this, "Error on invoking onOk() callback for asynchronous operation %s", e, this);
      }
    }
  }

  private void onError(final Exception e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(e);
      } catch (Exception e1) {
        PLogManager.instance().error(this, "Error on invoking onError() callback for asynchronous operation %s", e, this);
      }
    }
  }

}
