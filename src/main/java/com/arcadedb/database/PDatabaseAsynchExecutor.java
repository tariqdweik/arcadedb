package com.arcadedb.database;

import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PRawRecordCallback;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PLogManager;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PDatabaseAsynchExecutor {
  private final PDatabaseInternal                 database;
  private       DatabaseCreateRecordAsyncThread[] executorThreads;
  private       DatabaseScanAsyncThread[]         scanThreads;
  private int parallelLevel = -1;
  private int commitEvery   = 10000;

  // SPECIAL COMMANDS
  private final static String FORCE_COMMIT = "COMMIT";
  private final static String FORCE_EXIT   = "EXIT";

  private class DatabaseCreateRecordAsyncThread extends Thread {
    public final ArrayBlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(2048);
    public final PDatabaseInternal database;
    public final int               commitEvery;
    public volatile boolean shutdown      = false;
    public volatile boolean forceShutdown = false;
    public          long    count         = 0;

    private DatabaseCreateRecordAsyncThread(final PDatabaseInternal database, final int commitEvery, final int id) {
      super("AsyncCreateRecord-" + id);
      this.database = database;
      this.commitEvery = commitEvery;
    }

    @Override
    public void run() {
      if (PTransactionTL.INSTANCE.get() == null)
        PTransactionTL.INSTANCE.set(new PTransactionContext(database));

      database.getTransaction().begin();
      while (!forceShutdown) {
        try {
          final Object[] message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            if (message[0] == FORCE_COMMIT) {
              // COMMIT SPECIAL CASE
              database.getTransaction().commit();
              database.getTransaction().begin();

            } else if (message[0] == FORCE_EXIT) {
              break;

            } else {

              final PRecord record = (PRecord) message[0];
              final PBucket bucket = (PBucket) message[1];

              database.createRecordNoLock(record, bucket.getName());

              if (record instanceof PModifiableDocument) {
                final PModifiableDocument doc = (PModifiableDocument) record;
                database.indexDocument(doc, database.getSchema().getType(doc.getType()), bucket);
              }

              count++;

              if (count % commitEvery == 0) {
                database.getTransaction().commit();
                database.getTransaction().begin();
              }
            }
          } else if (shutdown)
            break;

        } catch (PConcurrentModificationException e) {
          PLogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
          database.getTransaction().begin();

        } catch (InterruptedException e) {
          queue.clear();
          break;
        } catch (Exception e) {
          PLogManager.instance().error(this, "Error on saving record (asyncThread=%s)", e, getName());
        }
      }
      database.getTransaction().commit();
    }
  }

  private class DatabaseScanAsyncThread extends Thread {
    public final ArrayBlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(2048);
    public final PDatabase database;
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
              final CountDownLatch semaphore = (CountDownLatch) message[0];
              final PRecordCallback userCallback = (PRecordCallback) message[1];
              final PBucket bucket = (PBucket) message[2];

              try {
                bucket.scan(new PRawRecordCallback() {
                  @Override
                  public boolean onRecord(final PRID rid, final PBinary view) {
                    if (shutdown)
                      return false;

                    final PRecord record = database.getRecordFactory()
                        .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);
                    return userCallback.onRecord(record);
                  }
                });
              } finally {
                // UNLOCK THE CALLER THREAD
                semaphore.countDown();
              }
            }
          }

        } catch (InterruptedException e) {
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

  public PDatabaseAsynchExecutor(final PDatabaseInternal database) {
    this.database = database;
    createThreads(Runtime.getRuntime().availableProcessors());
  }

  public PDBAsynchStats getStats() {
    final PDBAsynchStats stats = new PDBAsynchStats();
    stats.queueSize = 0;

    if (executorThreads != null)
      for (int i = 0; i < executorThreads.length; ++i)
        stats.queueSize += executorThreads[i].queue.size();

    if (scanThreads != null)
      for (int i = 0; i < scanThreads.length; ++i)
        stats.queueSize += scanThreads[i].queue.size();

    return stats;
  }

  public void waitCompletion() {
    while (true) {
      if (executorThreads != null) {
        int completed = 0;
        for (int i = 0; i < executorThreads.length; ++i) {
          try {
            executorThreads[i].queue.put(new Object[] { FORCE_COMMIT });
            if (executorThreads[i].queue.isEmpty())
              ++completed;
            else
              break;
          } catch (InterruptedException e) {
            break;
          }
        }

        if (completed < executorThreads.length) {
          try {
            Thread.sleep(100);
            continue;
          } catch (InterruptedException e) {
            return;
          }
        }
      }

      if (scanThreads != null) {
        int completed = 0;
        for (int i = 0; i < scanThreads.length; ++i) {
          if (scanThreads[i].queue.isEmpty())
            ++completed;
          else
            break;
        }

        if (completed < scanThreads.length) {
          try {
            Thread.sleep(100);
            continue;
          } catch (InterruptedException e) {
            return;
          }
        }

        return;
      }
    }
  }

  public void scanType(final String typeName, final PDocumentCallback callback) {
    try {
      final PDocumentType type = database.getSchema().getType(typeName);

      final List<PBucket> buckets = type.getBuckets();
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (PBucket b : type.getBuckets()) {
        final int slot = b.getId() % parallelLevel;

        try {
          scanThreads[slot].queue.put(new Object[] { semaphore, callback, b });
        } catch (InterruptedException e) {
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
        executorThreads[slot].queue.put(new Object[] { record, bucket });
      } catch (InterruptedException e) {
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
        executorThreads[slot].queue.put(new Object[] { record, bucket });
      } catch (InterruptedException e) {
        throw new PDatabaseOperationException("Error on executing save");
      }
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  public void close() {
    shutdownThreads();
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

  private void createThreads(final int parallelLevel) {
    shutdownThreads();

    executorThreads = new DatabaseCreateRecordAsyncThread[parallelLevel];
    scanThreads = new DatabaseScanAsyncThread[parallelLevel];

    for (int i = 0; i < parallelLevel; ++i) {
      executorThreads[i] = new DatabaseCreateRecordAsyncThread(database, this.commitEvery, i);
      executorThreads[i].start();

      scanThreads[i] = new DatabaseScanAsyncThread(database, i);
      scanThreads[i].start();
    }

    this.parallelLevel = parallelLevel;
  }

  private void shutdownThreads() {
    if (executorThreads != null) {
      // WAIT FOR SHUTDOWN, MAX 1S EACH
      for (int i = 0; i < executorThreads.length; ++i)
        try {
          executorThreads[i].shutdown = true;
          executorThreads[i].queue.put(new Object[] { FORCE_EXIT });
          executorThreads[i].join(10000);
        } catch (InterruptedException e) {
          // IGNORE IT
        }
    }

    if (scanThreads != null) {
      // WAIT FOR SHUTDOWN, MAX 1S EACH
      for (int i = 0; i < scanThreads.length; ++i)
        try {
          scanThreads[i].shutdown = true;
          scanThreads[i].queue.put(new Object[] { FORCE_EXIT });
          scanThreads[i].join(10000);
        } catch (InterruptedException e) {
          // IGNORE IT
        }
    }
  }
}
