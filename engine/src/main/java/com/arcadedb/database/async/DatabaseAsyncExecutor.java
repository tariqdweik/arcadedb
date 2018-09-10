/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.*;
import com.arcadedb.index.Index;
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
  private final DatabaseInternal   database;
  private       AsyncThread[]      executorThreads;
  private       int                parallelLevel          = 1;
  private       int                commitEvery;
  private       boolean            transactionUseWAL      = true;
  private       WALFile.FLUSH_TYPE transactionSync        = WALFile.FLUSH_TYPE.NO;
  private       AtomicLong         transactionCounter     = new AtomicLong();
  private       AtomicLong         commandRoundRobinIndex = new AtomicLong();

  // SPECIAL TASKS
  public final static DatabaseAsyncTask FORCE_EXIT = new DatabaseAsyncAbstractTask() {
    @Override
    public String toString() {
      return "FORCE_EXIT";
    }
  };

  private OkCallback    onOkCallback;
  private ErrorCallback onErrorCallback;

  private class AsyncThread extends Thread {
    public final    PushPullBlockingQueue<DatabaseAsyncTask> queue;
    public final    DatabaseInternal                         database;
    public volatile boolean                                  shutdown      = false;
    public volatile boolean                                  forceShutdown = false;
    public          long                                     count         = 0;

    private AsyncThread(final DatabaseInternal database, final int id) {
      super("AsyncExecutor-" + id);
      this.database = database;
      this.queue = new PushPullBlockingQueue<>(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE) / parallelLevel);
    }

    @Override
    public void run() {
      DatabaseContext.INSTANCE.init(database);

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).asyncMode = true;
      database.getTransaction().setUseWAL(transactionUseWAL);
      database.getTransaction().setWALFlush(transactionSync);
      database.getTransaction().begin();

      while (!forceShutdown) {
        try {
          final DatabaseAsyncTask message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            LogManager.instance().debug(this, "Received async message %s (threadId=%d)", message, Thread.currentThread().getId());

            if (message == FORCE_EXIT) {

              break;

            } else if (message instanceof DatabaseAsyncCompletion) {
              try {
                database.commit();
                onOk();
              } catch (Exception e) {
                onError(e);
              }
              database.begin();

            } else if (message instanceof DatabaseAsyncTransaction) {
              final DatabaseAsyncTransaction task = (DatabaseAsyncTransaction) message;

              ConcurrentModificationException lastException = null;

              if (database.isTransactionActive())
                database.commit();

              for (int retry = 0; retry < task.retries + 1; ++retry) {
                try {
                  database.begin();
                  task.tx.execute(database);
                  database.commit();

                  lastException = null;

                  // OK
                  break;

                } catch (ConcurrentModificationException e) {
                  // RETRY
                  lastException = e;

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
              final DatabaseAsyncCreateRecord task = (DatabaseAsyncCreateRecord) message;

              beginTxIfNeeded();

              try {

                database.createRecordNoLock(task.record, task.bucket.getName());

                if (task.record instanceof MutableDocument) {
                  final MutableDocument doc = (MutableDocument) task.record;
                  database.getIndexer().createDocument(doc, database.getSchema().getType(doc.getType()), task.bucket);
                }

                count++;

                if (count % commitEvery == 0) {
                  database.commit();
                  onOk();
                  database.begin();
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
                  database.commit();
                }

                if (sql.userCallback != null)
                  sql.userCallback.onOk(resultset);

              } catch (Exception e) {
                if (sql.userCallback != null)
                  sql.userCallback.onError(e);
              } finally {
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof DatabaseAsyncScanBucket) {

              final DatabaseAsyncScanBucket task = (DatabaseAsyncScanBucket) message;

              try {
                task.bucket.scan((rid, view) -> {
                  if (shutdown)
                    return false;

                  final Record record = database.getRecordFactory()
                      .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);

                  return task.userCallback.onRecord((Document) record);
                });
              } finally {
                // UNLOCK THE CALLER THREAD
                task.semaphore.countDown();
              }
            } else if (message instanceof DatabaseAsyncCreateOutEdge) {

              final DatabaseAsyncCreateOutEdge task = (DatabaseAsyncCreateOutEdge) message;

              try {
                beginTxIfNeeded();

                RID outEdgesHeadChunk = task.sourceVertex.getOutEdgesHeadChunk();

                final VertexInternal modifiableSourceVertex;
                if (outEdgesHeadChunk == null) {
                  final MutableEdgeChunk outChunk = new MutableEdgeChunk(database, GraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(outChunk,
                      GraphEngine.getEdgesBucketName(database, task.sourceVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
                  outEdgesHeadChunk = outChunk.getIdentity();

                  modifiableSourceVertex = (VertexInternal) task.sourceVertex.modify();
                  modifiableSourceVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableSourceVertex);
                } else
                  modifiableSourceVertex = task.sourceVertex;

                final EdgeLinkedList outLinkedList = new EdgeLinkedList(modifiableSourceVertex, Vertex.DIRECTION.OUT,
                    (EdgeChunk) database.lookupByRID(modifiableSourceVertex.getOutEdgesHeadChunk(), true));

                outLinkedList.add(task.edgeRID, task.destinationVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof DatabaseAsyncCreateInEdge) {

              final DatabaseAsyncCreateInEdge task = (DatabaseAsyncCreateInEdge) message;

              try {
                beginTxIfNeeded();

                RID inEdgesHeadChunk = task.destinationVertex.getInEdgesHeadChunk();

                final VertexInternal modifiableDestinationVertex;
                if (inEdgesHeadChunk == null) {
                  final MutableEdgeChunk inChunk = new MutableEdgeChunk(database, GraphEngine.EDGES_LINKEDLIST_CHUNK_SIZE);
                  database.createRecordNoLock(inChunk,
                      GraphEngine.getEdgesBucketName(database, task.destinationVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
                  inEdgesHeadChunk = inChunk.getIdentity();

                  modifiableDestinationVertex = (VertexInternal) task.destinationVertex.modify();
                  modifiableDestinationVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
                  database.updateRecordNoLock(modifiableDestinationVertex);
                } else
                  modifiableDestinationVertex = task.destinationVertex;

                final EdgeLinkedList inLinkedList = new EdgeLinkedList(modifiableDestinationVertex, Vertex.DIRECTION.IN,
                    (EdgeChunk) database.lookupByRID(modifiableDestinationVertex.getInEdgesHeadChunk(), true));

                inLinkedList.add(task.edgeRID, task.sourceVertexRID);

              } catch (Exception e) {
                onError(e);
                if (!database.isTransactionActive())
                  database.begin();
              }

            } else if (message instanceof DatabaseAsyncIndexCompaction) {

              final DatabaseAsyncIndexCompaction task = (DatabaseAsyncIndexCompaction) message;

              if (database.isTransactionActive())
                database.commit();

              try {
                task.index.compact();
              } catch (Exception e) {
                LogManager.instance().error(this, "Error on executing compaction of index '%s'", e, task.index.getName());
              }

              beginTxIfNeeded();
            }

            message.completed();

          } else if (shutdown)
            break;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          queue.clear();
          break;
        } catch (Exception e) {
          LogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
          if (!database.getTransaction().isActive())
            database.begin();
        }
      }

      try {
        database.commit();
        onOk();
      } catch (Exception e) {
        onError(e);
      }
    }
  }

  public DatabaseAsyncExecutor(final DatabaseInternal database) {
    this.database = database;
    this.commitEvery = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_TX_BATCH_SIZE);
    createThreads(Runtime.getRuntime().availableProcessors() - 1);
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

  public void setTransactionSync(final WALFile.FLUSH_TYPE transactionSync) {
    this.transactionSync = transactionSync;
    createThreads(parallelLevel);
  }

  public void onOk(final OkCallback callback) {
    onOkCallback = callback;
  }

  public void onError(final ErrorCallback callback) {
    onErrorCallback = callback;
  }

  public void compact(final Index index) {
    if (index.scheduleCompaction())
      scheduleTask(getFreeSlot(), new DatabaseAsyncIndexCompaction(index));
  }

  /**
   * Looks for an empty queue or the queue with less messages.
   */
  private int getFreeSlot() {
    int minQueueSize = 0;
    int minQueueIndex = 0;
    for (int i = 0; i < executorThreads.length; ++i) {
      final int qSize = executorThreads[i].queue.size();
      if (qSize == 0)
        // EMPTY QUEUE, USE THIS
        return i;

      if (qSize < minQueueSize) {
        minQueueSize = qSize;
        minQueueIndex = i;
      }
    }

    return minQueueIndex;
  }

  /**
   * Waits for the completion of all the pending tasks.
   */
  public void waitCompletion() {
    if (executorThreads == null)
      return;

    final DatabaseAsyncCompletion[] semaphores = new DatabaseAsyncCompletion[executorThreads.length];

    for (int i = 0; i < executorThreads.length; ++i)
      try {
        semaphores[i] = new DatabaseAsyncCompletion();
        executorThreads[i].queue.put(semaphores[i]);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

    for (int i = 0; i < semaphores.length; ++i)
      try {
        semaphores[i].waitForCompletition(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
  }

  public void command(final String language, final String query, final Map<String, Object> args, final SQLCallback callback) {
    // TODO: SUPPORT MULTIPLE LANGUAGES
    final int slot = (int) (commandRoundRobinIndex.getAndIncrement() % executorThreads.length);
    scheduleTask(slot, new DatabaseAsyncSQL(query, args, callback));
  }

  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    try {
      final DocumentType type = database.getSchema().getType(typeName);

      final List<Bucket> buckets = type.getBuckets(polymorphic);
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (Bucket b : buckets) {
        final int slot = b.getId() % parallelLevel;
        scheduleTask(slot, new DatabaseAsyncScanBucket(semaphore, callback, b));
      }

      semaphore.await();

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
    }
  }

  public void transaction(final Database.TransactionScope txBlock) {
    transaction(txBlock, database.getConfiguration().getValueAsInteger(GlobalConfiguration.MVCC_RETRIES));
  }

  public void transaction(final Database.TransactionScope txBlock, final int retries) {
    scheduleTask((int) (transactionCounter.getAndIncrement() % executorThreads.length), new DatabaseAsyncTransaction(txBlock, retries));
  }

  public void createRecord(final MutableDocument record) {
    final DocumentType type = database.getSchema().getType(record.getType());

    if (record.getIdentity() == null) {
      // NEW

      final Bucket bucket = type.getBucketToSave(false);
      final int slot = bucket.getId() % parallelLevel;

      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket));

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void createRecord(final Record record, final String bucketName) {
    final Bucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = bucket.getId() % parallelLevel;

    if (record.getIdentity() == null)
      // NEW
      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket));
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  /**
   * The current thread executes 2 lookups + create the edge. The creation of the 2 edge branches are delegated to asynchronous operations.
   */
  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue, final String destinationVertexType,
      final String[] destinationVertexKey, final Object[] destinationVertexValue, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final NewEdgeCallback callback, final Object... properties) {
    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<RID> v1Result = database.lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);

    boolean createdSourceVertex = false;

    VertexInternal sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = database.newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKey.length; ++i)
          ((MutableVertex) sourceVertex).set(sourceVertexKey[i], sourceVertexValue[i]);

        ((MutableVertex) sourceVertex).save();
        createdSourceVertex = true;

      } else
        throw new IllegalArgumentException("Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));
    } else
      sourceVertex = (VertexInternal) v1Result.next().getRecord();

    boolean createdDestinationVertex = false;

    final Iterator<RID> v2Result = database.lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);
    VertexInternal destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = database.newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKey.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKey[i], destinationVertexValue[i]);

        ((MutableVertex) destinationVertex).save();
        createdDestinationVertex = true;

      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays.toString(destinationVertexValue));
    } else
      destinationVertex = (VertexInternal) v2Result.next().getRecord();

    newEdge(sourceVertex, edgeType, destinationVertex, bidirectional, createdSourceVertex, createdDestinationVertex, callback, properties);
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

  public class PDBAsynchStats {
    public long queueSize;
  }

  private void beginTxIfNeeded() {
    if (!database.getTransaction().isActive())
      database.begin();
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
      final boolean createdSourceVertex, final boolean createdDestinationVertex, final NewEdgeCallback callback, final Object... properties) {
    if (destinationVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID rid = sourceVertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (destinationVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) sourceVertex.getDatabase();

    try {
      final MutableEdge edge = new MutableEdge(database, edgeType, rid, destinationVertex.getIdentity());
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

      if (callback != null)
        callback.call(edge, createdSourceVertex, createdDestinationVertex);

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

  private void scheduleTask(final int slot, final DatabaseAsyncTask task) {
    try {
      executorThreads[slot].queue.put(task);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing asynchronous task " + task);
    }
  }
}
