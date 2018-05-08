package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRecord;
import com.arcadedb.engine.PDatabaseChecker;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PVertexType;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultSet;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PPair;
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RandomTestMultiThreads {
  private static final int CYCLES           = 20000;
  private static final int STARTING_ACCOUNT = 100;
  private static final int PARALLEL         = Runtime.getRuntime().availableProcessors();
  private static final int WORKERS          = Runtime.getRuntime().availableProcessors() * 8;

  private final AtomicLong                      total             = new AtomicLong();
  private final AtomicLong                      totalTransactions = new AtomicLong();
  private final AtomicLong                      mvccErrors        = new AtomicLong();
  private final Random                          rnd               = new Random();
  private final AtomicLong                      uuid              = new AtomicLong();
  private final List<PPair<Integer, Exception>> otherErrors       = Collections.synchronizedList(new ArrayList<>());

  @Test
  public void testRandom() {
    PLogManager.instance().info(this, "Executing " + CYCLES + " transactions with %d workers", WORKERS);

    PerformanceTest.clean();
    createSchema();
    populateDatabase();

    long begin = System.currentTimeMillis();

    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {

      final Thread[] threads = new Thread[WORKERS];
      for (int i = 0; i < WORKERS; ++i) {
        final int threadId = i;
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            database.begin();

            long totalTransactionInCurrentTx = 0;

            while (true) {
              final long i = total.incrementAndGet();
              if (i >= CYCLES)
                break;

              try {
                final int op = rnd.nextInt(25);
                if (i % 5000 == 0)
                  PLogManager.instance()
                      .info(this, "Operations %d/%d totalTransactionInCurrentTx=%d totalTransactions=%d (thread=%d)", i, CYCLES,
                          totalTransactionInCurrentTx, totalTransactions.get(), threadId);

                PLogManager.instance().debug(this, "Operation %d %d/%d (thread=%d)", op, i, CYCLES, threadId);

                if (op >= 0 && op <= 5) {
                  final int txOps = rnd.nextInt(10);
                  PLogManager.instance().debug(this, "Creating %d transactions (thread=%d)...", txOps, threadId);

                  createTransactions(database, txOps);
                  totalTransactionInCurrentTx += txOps;

                } else if (op >= 6 && op <= 10) {
                  PLogManager.instance().debug(this, "Querying Account by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":id", rnd.nextInt(10000) + 1);

                  final OResultSet result = database.query("select from Account where id = :id", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }

                } else if (op >= 11 && op <= 14) {
                  PLogManager.instance().debug(this, "Querying Transaction by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":uuid", rnd.nextInt((int) (totalTransactions.get() + 1)) + 1);

                  final OResultSet result = database.query("select from Transaction where uuid = :uuid", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }
                } else if (op == 15) {
                  PLogManager.instance().debug(this, "Scanning Account records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", rnd.nextInt(100) + 1);

                  final OResultSet result = database.query("select from Account limit :limit", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }

                } else if (op == 16) {
                  PLogManager.instance().debug(this, "Scanning Transaction records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", rnd.nextInt((int) totalTransactions.get() + 1) + 1);

                  final OResultSet result = database.query("select from Transaction limit :limit", map);
                  while (result.hasNext()) {
                    final OResult record = result.next();
                    record.toString();
                  }

                } else if (op == 17) {
                  PLogManager.instance().debug(this, "Deleting records (thread=%d)...", threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op >= 18 && op <= 19) {

                  PLogManager.instance().debug(this, "Committing (thread=%d)...", threadId);
                  database.commit();

                  totalTransactions.addAndGet(totalTransactionInCurrentTx);
                  totalTransactionInCurrentTx = 0;

                  database.begin();
                } else if (op >= 20 && op <= 24) {

                  PLogManager.instance().debug(this, "Updating records (thread=%d)...", threadId);

                  updateRecords(database, threadId);

                }

              } catch (Exception e) {
                if (e instanceof PConcurrentModificationException) {
                  mvccErrors.incrementAndGet();
                  total.decrementAndGet();
                  totalTransactionInCurrentTx = 0;
                } else {
                  otherErrors.add(new PPair<>(threadId, e));
                  PLogManager.instance().error(this, "UNEXPECTED ERROR: " + e, e);
                }

                if (!database.isTransactionActive())
                  database.begin();
              }
            }

            try {
              database.commit();
            } catch (Exception e) {
              mvccErrors.incrementAndGet();
            }

          }
        });
        threads[i].start();
      }

      PLogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");

      for (int i = 0; i < WORKERS; ++i) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    } finally {
      new PDatabaseChecker().check(database);

      database.close();

      System.out.println("Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get()
          + " otherExceptions=" + otherErrors.size());

      for (PPair<Integer, Exception> entry : otherErrors) {
        System.out.println(" = threadId=" + entry.getFirst() + " exception=" + entry.getSecond());
      }
    }
  }

  private void createTransactions(final PDatabase database, final int txOps) {
    for (long txId = 0; txId < txOps; ++txId) {
      final PModifiableDocument tx = database.newVertex("Transaction");
      tx.set("uuid", "" + uuid.getAndIncrement());
      tx.set("date", new Date());
      tx.set("amount", rnd.nextInt(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private int updateRecords(final PDatabase database, final int threadId) {
    if (totalTransactions.get() == 0)
      return 0;

    final Iterator<PRecord> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = rnd.nextInt((int) totalTransactions.get() + 1 / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int updated = 0;

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final PRecord next = iter.next();

      if (rnd.nextInt(2) == 0) {
        try {
          final PModifiableDocument doc = (PModifiableDocument) next.modify();

          Integer val = (Integer) doc.get("updated");
          if (val == null)
            val = 0;
          doc.set("updated", val + 1);

          if (rnd.nextInt(2) == 1)
            doc.set("longFieldUpdated", "This is a long field to test the break of pages");

          updated++;

        } catch (PRecordNotFoundException e) {
          // OK
        }
        PLogManager.instance().debug(this, "Updated record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return updated;
  }

  private int deleteRecords(final PDatabase database, final int threadId) {
    if (totalTransactions.get() == 0)
      return 0;

    final Iterator<PRecord> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = rnd.nextInt((int) totalTransactions.get() + 1 / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int deleted = 0;

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final PRecord next = iter.next();

      if (rnd.nextInt(2) == 0) {
        try {
          database.deleteRecord(next);
          deleted++;
        } catch (PRecordNotFoundException e) {
          // OK
        }
        PLogManager.instance().debug(this, "Deleted record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return deleted;
  }

  private void populateDatabase() {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    database.begin();

    try {
      for (long row = 0; row < STARTING_ACCOUNT; ++row) {
        final PModifiableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        record.save();
      }

      database.commit();

    } finally {
      database.close();
      PLogManager.instance().info(this, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType("Account")) {
        database.begin();

        final PVertexType accountType = database.getSchema().createVertexType("Account", PARALLEL);
        accountType.createProperty("id", Long.class);
        accountType.createProperty("name", String.class);
        accountType.createProperty("surname", String.class);
        accountType.createProperty("registered", Date.class);

        database.getSchema().createClassIndexes("Account", new String[] { "id" }, 500000);

        final PVertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
        txType.createProperty("uuid", String.class);
        txType.createProperty("date", Date.class);
        txType.createProperty("amount", BigDecimal.class);

        database.getSchema().createClassIndexes("Transaction", new String[] { "uuid" }, 500000);

        final PEdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
        edgeType.createProperty("date", Date.class);

        database.commit();
      }
    } finally {
      database.close();
    }
  }
}


