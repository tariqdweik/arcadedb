/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class DatabaseAsyncExecutor {
  private final DatabaseInternal   database;
  private final Random             random                 = new Random();
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
    public void execute(AsyncThread async, DatabaseInternal database) {
    }

    @Override
    public String toString() {
      return "FORCE_EXIT";
    }
  };

  private OkCallback    onOkCallback;
  private ErrorCallback onErrorCallback;

  public class AsyncThread extends Thread {
    public final    BlockingQueue<DatabaseAsyncTask> queue;
    public final    DatabaseInternal                 database;
    public volatile boolean                          shutdown      = false;
    public volatile boolean                          forceShutdown = false;
    public          long                             count         = 0;

    private AsyncThread(final DatabaseInternal database, final int id) {
      super("AsyncExecutor-" + id);
      this.database = database;

      final int queueSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE) / parallelLevel;

      final String cfgQueueImpl = database.getConfiguration().getValueAsString(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL);
      if ("fast".equalsIgnoreCase(cfgQueueImpl))
        this.queue = new PushPullBlockingQueue<>(queueSize);
      else if ("standard".equalsIgnoreCase(cfgQueueImpl))
        this.queue = new ArrayBlockingQueue<>(queueSize);
      else {
        // WARNING AND THEN USE THE DEFAULT
        LogManager.instance().log(this, Level.WARNING, "Error on async operation queue implementation setting: %s is not supported", null, cfgQueueImpl);
        this.queue = new ArrayBlockingQueue<>(queueSize);
      }
    }

    public boolean isShutdown() {
      return shutdown;
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
            LogManager.instance().log(this, Level.FINE, "Received async message %s (threadId=%d)", null, message, Thread.currentThread().getId());

            if (message == FORCE_EXIT) {

              break;

            } else {

              try {
                if (message.requiresActiveTx() && !database.getTransaction().isActive())
                  database.begin();

                message.execute(this, database);

                count++;

                if (count % commitEvery == 0)
                  database.commit();

              } catch (Exception e) {
                onError(e);
              } finally {
                message.completed();

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
          LogManager.instance().log(this, Level.SEVERE, "Error on executing asynchronous operation (asyncThread=%s)", e, getName());
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

    public void onError(final Exception e) {
      DatabaseAsyncExecutor.this.onError(e);
    }

    public void onOk() {
      DatabaseAsyncExecutor.this.onOk();
    }
  }

  public DatabaseAsyncExecutor(final DatabaseInternal database) {
    this.database = database;
    this.commitEvery = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_TX_BATCH_SIZE);
    createThreads(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
  }

  public DBAsyncStats getStats() {
    final DBAsyncStats stats = new DBAsyncStats();
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
      scheduleTask(getBestSlot(), new DatabaseAsyncIndexCompaction(index), false);
  }

  /**
   * Looks for an empty queue or the queue with less messages.
   */
  private int getBestSlot() {
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
   * Returns a random slot.
   */
  private int getRandomSlot() {
    return random.nextInt(executorThreads.length);
  }

  public void waitCompletion() {
    waitCompletion(0l);
  }

  /**
   * Waits for the completion of all the pending tasks.
   */
  public void waitCompletion(long timeout) {
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
        if (timeout <= 0)
          timeout = Long.MAX_VALUE;

        semaphores[i].waitForCompetition(timeout);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
  }

  public void query(final String language, final String query, final AsyncResultsetCallback callback, final Object... parameters) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(true, language, query, parameters, callback), true);
  }

  public void query(final String language, final String query, final AsyncResultsetCallback callback, final Map<String, Object> parameters) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(true, language, query, parameters, callback), true);
  }

  public void command(final String language, final String query, final AsyncResultsetCallback callback, final Object... parameters) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(false, language, query, parameters, callback), true);
  }

  public void command(final String language, final String query, final AsyncResultsetCallback callback, final Map<String, Object> parameters) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(false, language, query, parameters, callback), true);
  }

  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    try {
      final DocumentType type = database.getSchema().getType(typeName);

      final List<Bucket> buckets = type.getBuckets(polymorphic);
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (Bucket b : buckets) {
        final int slot = getSlot(b.getId());
        scheduleTask(slot, new DatabaseAsyncScanBucket(semaphore, callback, b), true);
      }

      semaphore.await();

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
    }
  }

  public void transaction(final Database.TransactionScope txBlock) {
    transaction(txBlock, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES));
  }

  public void transaction(final Database.TransactionScope txBlock, final int retries) {
    transaction(txBlock, retries, null, null);
  }

  public void transaction(final Database.TransactionScope txBlock, final int retries, final OkCallback ok, final ErrorCallback error) {
    scheduleTask(getSlot((int) transactionCounter.getAndIncrement()), new DatabaseAsyncTransaction(txBlock, retries, ok, error), true);
  }

  public void createRecord(final MutableDocument record, final NewRecordCallback newRecordCallback) {
    final DocumentType type = database.getSchema().getType(record.getType());

    if (record.getIdentity() == null) {
      // NEW
      final Bucket bucket = type.getBucketToSave(false);
      final int slot = getSlot(bucket.getId());

      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback), true);

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void createRecord(final Record record, final String bucketName, final NewRecordCallback newRecordCallback) {
    final Bucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = getSlot(bucket.getId());

    if (record.getIdentity() == null)
      // NEW
      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback), true);
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void newEdge(final Vertex sourceVertex, final String edgeType, final RID destinationVertexRID, final boolean bidirectional, final boolean light,
      final NewEdgeCallback callback, final Object... properties) {
    if (sourceVertex == null)
      throw new IllegalArgumentException("Source vertex is null");

    if (destinationVertexRID == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final int sourceSlot = getSlot(sourceVertex.getIdentity().getBucketId());
    final int destinationSlot = getSlot(destinationVertexRID.getBucketId());

    if (sourceSlot == destinationSlot)
      // BOTH VERTICES HAVE THE SAME SLOT, CREATE THE EDGE USING IT
      scheduleTask(sourceSlot, new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, bidirectional, light, callback), true);
    else {
      // CREATE THE EDGE IN THE SOURCE VERTEX'S SLOT AND A CASCADE TASK TO ADD THE INCOMING EDGE FROM DESTINATION VERTEX (THIS IS THE MOST EXPENSIVE CASE WHERE 2 TASKS ARE EXECUTED)
      scheduleTask(sourceSlot, new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, false, light, new NewEdgeCallback() {
        @Override
        public void call(final Edge newEdge, final boolean createdSourceVertex, final boolean createdDestinationVertex) {
          if (bidirectional) {
            scheduleTask(destinationSlot, new CreateIncomingEdgeAsyncTask(sourceVertex.getIdentity(), destinationVertexRID, newEdge, new NewEdgeCallback() {
              @Override
              public void call(final Edge newEdge, final boolean createdSourceVertex, final boolean createdDestinationVertex) {
                if (callback != null)
                  callback.call(newEdge, createdSourceVertex, createdDestinationVertex);
              }
            }), true);
          } else if (callback != null)
            callback.call(newEdge, createdSourceVertex, createdDestinationVertex);

        }
      }), true);
    }
  }

  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue, final String destinationVertexType,
      final String[] destinationVertexKey, final Object[] destinationVertexValue, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final boolean light, final NewEdgeCallback callback, final Object... properties) {

    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> sourceResult = database.lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);
    final Iterator<Identifiable> destinationResult = database.lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);

    final RID sourceRID = sourceResult.hasNext() ? sourceResult.next().getIdentity() : null;
    final RID destinationRID = destinationResult.hasNext() ? destinationResult.next().getIdentity() : null;

    if (sourceRID == null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find source and destination vertices with respectively key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue)
                + " and " + Arrays.toString(destinationVertexKey) + "=" + Arrays.toString(destinationVertexValue));

      // SOURCE AND DESTINATION VERTICES BOTH DON'T EXIST: CREATE 2 VERTICES + EDGE IN THE SAME TASK PICKING THE BEST SLOT
      scheduleTask(getRandomSlot(),
          new CreateBothVerticesAndEdgeAsyncTask(sourceVertexType, sourceVertexKey, sourceVertexValue, destinationVertexType, destinationVertexKey,
              destinationVertexValue, edgeType, properties, bidirectional, light, callback), true);

    } else if (sourceRID != null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays.toString(destinationVertexValue));

      // ONLY SOURCE VERTEX EXISTS, CREATE DESTINATION VERTEX + EDGE IN SOURCE'S SLOT
      scheduleTask(getSlot(sourceRID.getBucketId()),
          new CreateDestinationVertexAndEdgeAsyncTask(sourceRID, destinationVertexType, destinationVertexKey, destinationVertexValue, edgeType, properties,
              bidirectional, light, callback), true);

    } else if (sourceRID == null && destinationRID != null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException("Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));

      // ONLY DESTINATION VERTEX EXISTS
      scheduleTask(getSlot(destinationRID.getBucketId()),
          new CreateSourceVertexAndEdgeAsyncTask(sourceVertexType, sourceVertexKey, sourceVertexValue, destinationRID, edgeType, properties, bidirectional,
              light, callback), true);

    } else
      // BOTH VERTICES EXIST
      newEdge(sourceRID.getVertex(true), edgeType, destinationRID, bidirectional, light, callback, properties);
  }

  /**
   * Test only API.
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

  public class DBAsyncStats {
    public long queueSize;
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

  protected void onOk() {
    if (onOkCallback != null) {
      try {
        onOkCallback.call();
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on invoking onOk() callback for asynchronous operation %s", e, this);
      }
    }
  }

  protected void onError(final Exception e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(e);
      } catch (Exception e1) {
        LogManager.instance().log(this, Level.SEVERE, "Error on invoking onError() callback for asynchronous operation %s", e, this);
      }
    }
  }

  public void scheduleTask(final int slot, final DatabaseAsyncTask task, final boolean waitIfQueueIsFull) {
    try {
      //LogManager.instance().log(this, Level.FINE, "Scheduling async task %s (slot=%d)", null, task, slot);

      if (waitIfQueueIsFull)
        executorThreads[slot].queue.put(task);
      else
        executorThreads[slot].queue.offer(task);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing asynchronous task " + task);
    }
  }

  public int getSlot(final int value) {
    return value % executorThreads.length;
  }
}
