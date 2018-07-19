/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.RawRecordCallback;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.*;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.LogManager;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseAsyncExecutor {
  private final DatabaseInternal database;
  private       AsyncThread[]    executorThreads;
  private       int              parallelLevel          = 1;
  private       int              commitEvery            = GlobalConfiguration.ASYNC_TX_BATCH_SIZE.getValueAsInteger();
  private       boolean          transactionUseWAL      = true;
  private       boolean          transactionSync        = false;
  private       AtomicLong       transactionCounter     = new AtomicLong();
  private       AtomicLong       commandRoundRobinIndex = new AtomicLong();

  // SPECIAL COMMANDS
  private final static DatabaseAsyncCommand FORCE_COMMIT = new DatabaseAsyncCommand() {
    @Override
    public String toString() {
      return "FORCE_COMMIT";
    }
  };
  private final static DatabaseAsyncCommand FORCE_EXIT   = new DatabaseAsyncCommand() {
    @Override
    public String toString() {
      return "FORCE_EXIT";
    }
  };

  private OkCallback    onOkCallback;
  private ErrorCallback onErrorCallback;

  private class AsyncThread extends Thread {
    public final    PushPullBlockingQueue<DatabaseAsyncCommand> queue         = new PushPullBlockingQueue<>(
        GlobalConfiguration.ASYNC_OPERATIONS_QUEUE.getValueAsInteger() / parallelLevel);
    public final    DatabaseInternal                            database;
    public volatile boolean                                     shutdown      = false;
    public volatile boolean                                     forceShutdown = false;
    public          long                                        count         = 0;

    private AsyncThread(final DatabaseInternal database, final int id) {
      super("AsyncCreateRecord-" + id);
      this.database = database;
    }

    @Override
    public void run() {
      if (DatabaseContext.INSTANCE.get() == null)
        DatabaseContext.INSTANCE.init(database);

      DatabaseContext.INSTANCE.get().asyncMode = true;
      database.getTransaction().setUseWAL(transactionUseWAL);
      database.getTransaction().setSync(transactionSync);
      database.getTransaction().begin();

      while (!forceShutdown) {
        try {
          final DatabaseAsyncCommand message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            LogManager.instance().debug(this, "Received async message %s (threadId=%d)", message, Thread.currentThread().getId());

            if (message == FORCE_COMMIT) {
              // COMMIT SPECIAL CASE
              try {
                database.commit();
                onOk();
              } catch (Exception e) {
                onError(e);
              }
              database.begin();

            } else if (message == FORCE_EXIT) {

              break;

            } else if (message instanceof DatabaseAsyncTransaction) {
              final DatabaseAsyncTransaction command = (DatabaseAsyncTransaction) message;

              ConcurrentModificationException lastException = null;

              if (database.isTransactionActive())
                database.commit();

              for (int retry = 0; retry < command.retries + 1; ++retry) {
                try {
                  database.begin();
                  command.tx.execute(database);
                  database.commit();

                  lastException = null;

                  // OK
                  break;

                } catch (ConcurrentModificationException e) {
                  // RETRY
                  lastException = e;
                  beginTxIfNeeded();

                  continue;
                } catch (Exception e) {
                  if (database.getTransaction().isActive())
                    database.rollback();

                  onError(e);

                  throw e;
                }
              }

              if (lastException != null)
                onError(lastException);

              beginTxIfNeeded();

            } else if (message instanceof DatabaseAsyncCreateRecord) {
              final DatabaseAsyncCreateRecord command = (DatabaseAsyncCreateRecord) message;

              beginTxIfNeeded();

              try {

                database.createRecordNoLock(command.record, command.bucket.getName());

                if (command.record instanceof ModifiableDocument) {
                  final ModifiableDocument doc = (ModifiableDocument) command.record;
                  database.indexDocument(doc, database.getSchema().getType(doc.getType()), command.bucket);
                }

                count++;

                if (count % commitEvery == 0) {
                  database.getTransaction().commit();
                  onOk();
                  database.getTransaction().begin();
                }

              } catch (Exception e) {
                LogManager.instance().error(this, "Error on executing async create operation (threadId=%d)", e, Thread.currentThread().getId());

                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof DatabaseAsyncSQL) {

              final DatabaseAsyncSQL sql = (DatabaseAsyncSQL) message;

              try {
                final ResultSet resultset = database.command("SQL", sql.command, sql.args);

                count++;

                if (count % commitEvery == 0) {
                  database.getTransaction().commit();
                }

                if (sql.userCallback != null)
                  sql.userCallback.onOk(resultset);

              } catch (Exception e) {
                if (sql.userCallback != null)
                  sql.userCallback.onError(e);
              } finally {
                if (!database.isTransactionActive())
                  database.getTransaction().begin();
              }

            } else if (message instanceof DatabaseAsyncScanBucket) {

              final DatabaseAsyncScanBucket command = (DatabaseAsyncScanBucket) message;

              try {
                command.bucket.scan(new RawRecordCallback() {
                  @Override
                  public boolean onRecord(final RID rid, final Binary view) {
                    if (shutdown)
                      return false;

                    final Record record = database.getRecordFactory()
                        .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);

                    return command.userCallback.onRecord((Document) record);
                  }
                });
              } finally {
                // UNLOCK THE CALLER THREAD
                command.semaphore.countDown();
              }
            } else if (message instanceof DatabaseAsyncCreateOutEdge) {

              final DatabaseAsyncCreateOutEdge command = (DatabaseAsyncCreateOutEdge) message;

              try {
                beginTxIfNeeded();

                RID outEdgesHeadChunk = command.sourceVertex.getOutEdgesHeadChunk();

                final VertexInternal modifiableSourceVertex;
                if (outEdgesHeadChunk == null) {
                  final ModifiableEdgeChunk outChunk = new ModifiableEdgeChunk(database, GraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(outChunk,
                      GraphEngine.getEdgesBucketName(database, command.sourceVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
                  outEdgesHeadChunk = outChunk.getIdentity();

                  modifiableSourceVertex = (VertexInternal) command.sourceVertex.modify();
                  modifiableSourceVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableSourceVertex);
                } else
                  modifiableSourceVertex = command.sourceVertex;

                final EdgeLinkedList outLinkedList = new EdgeLinkedList(modifiableSourceVertex, Vertex.DIRECTION.OUT,
                    (EdgeChunk) database.lookupByRID(modifiableSourceVertex.getOutEdgesHeadChunk(), true));

                outLinkedList.add(command.edgeRID, command.destinationVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof DatabaseAsyncCreateInEdge) {

              final DatabaseAsyncCreateInEdge command = (DatabaseAsyncCreateInEdge) message;

              try {
                beginTxIfNeeded();

                RID inEdgesHeadChunk = command.destinationVertex.getInEdgesHeadChunk();

                final VertexInternal modifiableDestinationVertex;
                if (inEdgesHeadChunk == null) {
                  final ModifiableEdgeChunk inChunk = new ModifiableEdgeChunk(database, GraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(inChunk,
                      GraphEngine.getEdgesBucketName(database, command.destinationVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
                  inEdgesHeadChunk = inChunk.getIdentity();

                  modifiableDestinationVertex = (VertexInternal) command.destinationVertex.modify();
                  modifiableDestinationVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableDestinationVertex);
                } else
                  modifiableDestinationVertex = command.destinationVertex;

                final EdgeLinkedList inLinkedList = new EdgeLinkedList(modifiableDestinationVertex, Vertex.DIRECTION.IN,
                    (EdgeChunk) database.lookupByRID(modifiableDestinationVertex.getInEdgesHeadChunk(), true));

                inLinkedList.add(command.edgeRID, command.sourceVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            }
          } else if (shutdown)
            break;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          queue.clear();
          break;
        } catch (Exception e) {
          LogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
          if (!database.getTransaction().isActive())
            database.getTransaction().begin();
        }
      }

      try

      {
        database.getTransaction().commit();
        onOk();
      } catch (Exception e)

      {
        onError(e);
      }
    }
  }

  private void beginTxIfNeeded() {
    if (!database.getTransaction().isActive())
      database.getTransaction().begin();
  }

  public class PDBAsynchStats {
    public long queueSize;
  }

  public DatabaseAsyncExecutor(final DatabaseInternal database) {
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

  public void onOk(final OkCallback callback) {
    onOkCallback = callback;
  }

  public void onError(final ErrorCallback callback) {
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
          LogManager.instance().debug(this, "Waiting for completion async thread %s found %d messages still to be processed", executorThreads[i], messages);
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

  public void command(String language, final String query, final Map<String, Object> args, final SQLCallback callback) {
    try {
      final int slot = (int) (commandRoundRobinIndex.getAndIncrement() % executorThreads.length);
      executorThreads[slot].queue.put(new DatabaseAsyncSQL(query, args, callback));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing sql command");
    }
  }

  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    try {
      final DocumentType type = database.getSchema().getType(typeName);

      final List<Bucket> buckets = type.getBuckets(polymorphic);
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (Bucket b : buckets) {
        final int slot = b.getId() % parallelLevel;

        try {
          executorThreads[slot].queue.put(new DatabaseAsyncScanBucket(semaphore, callback, b));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseOperationException("Error on executing save");
        }
      }

      semaphore.await();

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
    }
  }

  public void transaction(final Database.Transaction txBlock) {
    transaction(txBlock, GlobalConfiguration.MVCC_RETRIES.getValueAsInteger());
  }

  public void transaction(final Database.Transaction txBlock, final int retries) {
    try {
      executorThreads[(int) (transactionCounter.getAndIncrement() % executorThreads.length)].queue.put(new DatabaseAsyncTransaction(txBlock, retries));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing transaction");
    }
  }

  public void createRecord(final ModifiableDocument record) {
    final DocumentType type = database.getSchema().getType(record.getType());

    if (record.getIdentity() == null) {
      // NEW

      final Bucket bucket = type.getBucketToSave(false);
      final int slot = bucket.getId() % parallelLevel;

      try {
        executorThreads[slot].queue.put(new DatabaseAsyncCreateRecord(record, bucket));

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseOperationException("Error on executing create record");
      }

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void createRecord(final Record record, final String bucketName) {
    final Bucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = bucket.getId() % parallelLevel;

    if (record.getIdentity() == null)
      // NEW
      try {
        executorThreads[slot].queue.put(new DatabaseAsyncCreateRecord(record, bucket));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseOperationException("Error on executing create record");
      }
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  /**
   * The current thread executes 2 lookups + create the edge. The creation of the 2 edge branches are delegated to asynchronous operations.
   */
  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue, final String destinationVertexType,
      final String[] destinationVertexKey, final Object[] destinationVertexValue, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<RID> v1Result = database.lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);

    VertexInternal sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = database.newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKey.length; ++i)
          ((ModifiableVertex) sourceVertex).set(sourceVertexKey[i], sourceVertexValue[i]);
      } else
        throw new IllegalArgumentException("Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));
    } else
      sourceVertex = (VertexInternal) v1Result.next().getRecord();

    final Iterator<RID> v2Result = database.lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);
    VertexInternal destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = database.newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKey.length; ++i)
          ((ModifiableVertex) destinationVertex).set(destinationVertexKey[i], destinationVertexValue[i]);
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays.toString(destinationVertexValue));
    } else
      destinationVertex = (VertexInternal) v2Result.next().getRecord();

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

  private void newEdge(VertexInternal sourceVertex, final String edgeType, VertexInternal destinationVertex, final boolean bidirectional,
      final Object... properties) {
    if (destinationVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID rid = sourceVertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (destinationVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) sourceVertex.getDatabase();

    try {
      final ModifiableEdge edge = new ModifiableEdge(database, edgeType, rid, destinationVertex.getIdentity());
      GraphEngine.setProperties(edge, properties);
      edge.save();

      try {
        executorThreads[rid.getBucketId() % parallelLevel].queue
            .put(new DatabaseAsyncCreateOutEdge(sourceVertex, edge.getIdentity(), destinationVertex.getIdentity()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseOperationException("Error on creating edge link from out to in");
      }

      if (bidirectional)
        try {
          executorThreads[destinationVertex.getIdentity().getBucketId() % parallelLevel].queue
              .put(new DatabaseAsyncCreateInEdge(destinationVertex, edge.getIdentity(), sourceVertex.getIdentity()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabaseOperationException("Error on creating edge link from out to in");
        }

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on creating edge", e);
    }
  }

  private void onOk() {
    if (onOkCallback != null) {
      try {
        onOkCallback.call();
      } catch (Exception e) {
        LogManager.instance().error(this, "Error on invoking onOk() callback for asynchronous operation %s", e, this);
      }
    }
  }

  private void onError(final Exception e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(e);
      } catch (Exception e1) {
        LogManager.instance().error(this, "Error on invoking onError() callback for asynchronous operation %s", e, this);
      }
    }
  }

}
