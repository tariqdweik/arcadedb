package com.arcadedb.database;

import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PFile;
import com.arcadedb.exception.PDatabaseIsReadOnlyException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.schema.PType;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PDatabaseParallel extends PDatabaseImpl {
  private DatabaseThread[] executorThread;
  private int parallelLevel = -1;
  private int commitEvery   = 10000;

  private class DatabaseThread extends Thread {
    public final ArrayBlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(2048);
    public final PDatabase database;
    public final int       commitEvery;
    public volatile boolean shutdown      = false;
    public volatile boolean forceShutdown = false;
    public          long    count         = 0;

    private DatabaseThread(final PDatabaseParallel database, final int commitEvery) {
      this.database = database;
      this.commitEvery = commitEvery;
    }

    @Override
    public void run() {
      if (PTransactionTL.INSTANCE.get() == null)
        PTransactionTL.INSTANCE.set(new PTransactionContext(database));

      getTransaction().begin();
      while (!forceShutdown) {
        try {
          final Object[] message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            final PRecord record = (PRecord) message[0];
            final PBucket bucket = (PBucket) message[1];
            ((PRecordInternal) record).setIdentity(bucket.addRecord(record));
            if (record instanceof PModifiableDocument) {
              final PModifiableDocument doc = (PModifiableDocument) record;
              indexRecord(doc, getSchema().getType(doc.getType()), bucket);
            }
            count++;

            if (count % commitEvery == 0) {
              getTransaction().commit();
              getTransaction().begin();
            }
          } else if (shutdown)
            break;

        } catch (InterruptedException e) {
          queue.clear();
          break;
        }
      }
      getTransaction().commit();
    }
  }

  protected PDatabaseParallel(final String path, final PFile.MODE mode, final boolean multiThread) {
    super(path, mode, multiThread);
    createThreads(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void saveRecord(final PModifiableDocument record) {
    lock();
    try {

      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot save record");

      final PType type = schema.getType(record.getType());
      if (type == null)
        throw new PDatabaseOperationException("Cannot save document because has no type");

      if (record.getIdentity() == null) {
        // NEW
        final PBucket bucket = type.getBucketToSave();
        final int slot = bucket.getId() % parallelLevel;

        try {
          executorThread[slot].queue.put(new Object[] { record, bucket });
        } catch (InterruptedException e) {
          throw new PDatabaseOperationException("Error on executing save");
        }

      } else {
        // UPDATE
        // TODO
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void saveRecord(final PRecord record, final String bucketName) {
    lock();
    try {

      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot save record");

      final PBucket bucket = schema.getBucketByName(bucketName);
      final int slot = bucket.getId() % parallelLevel;

      if (record.getIdentity() == null)
        // NEW
        try {
          executorThread[slot].queue.put(new Object[] { record, bucket });
        } catch (InterruptedException e) {
          throw new PDatabaseOperationException("Error on executing save");
        }
      else {
        // UPDATE
        // TODO
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void close() {
    lock();
    try {

      if (!open)
        return;

      shutdownThreads();

      super.close();

    } finally {
      unlock();
    }
  }

  public int getParallelLevel() {
    return parallelLevel;
  }

  public void setParallelLevel(final int parallelLevel) {
    if (parallelLevel != this.parallelLevel) {
      createThreads(parallelLevel);
    }
  }

  public int getCommitEvery() {
    return commitEvery;
  }

  public void setCommitEvery(final int commitEvery) {
    this.commitEvery = commitEvery;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PDatabaseParallel pDatabase = (PDatabaseParallel) o;

    return databasePath != null ? databasePath.equals(pDatabase.databasePath) : pDatabase.databasePath == null;
  }

  private void createThreads(final int parallelLevel) {
    shutdownThreads();

    executorThread = new DatabaseThread[parallelLevel];

    for (int i = 0; i < parallelLevel; ++i) {
      executorThread[i] = new DatabaseThread(this, this.commitEvery);
      executorThread[i].start();
    }

    this.parallelLevel = parallelLevel;
  }

  private void shutdownThreads() {
    if (executorThread != null) {
      for (int i = 0; i < executorThread.length; ++i)
        executorThread[i].shutdown = true;

      // WAIT FOR SHUTDOWN, MAX 1S EACH
      for (int i = 0; i < executorThread.length; ++i)
        try {
          executorThread[i].join(1000);
        } catch (InterruptedException e) {
          // IGNORE IT
        }
    }
  }
}
