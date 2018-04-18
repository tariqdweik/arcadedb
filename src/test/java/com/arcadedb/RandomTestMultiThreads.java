package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRecord;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PVertexType;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PPair;
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RandomTestMultiThreads {
  private static final int CYCLES           = 1000000;
  private static final int STARTING_ACCOUNT = 100;
  private static final int PARALLEL         = Runtime.getRuntime().availableProcessors();
  private static final int WORKERS          = Runtime.getRuntime().availableProcessors() * 8;

  private final AtomicLong                      total       = new AtomicLong();
  private final AtomicLong                      mvccErrors  = new AtomicLong();
  private final Random                          rnd         = new Random();
  private final AtomicLong                      uuid        = new AtomicLong();
  private final List<PPair<Integer, Exception>> otherErrors = Collections.synchronizedList(new ArrayList<>());

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

            while (true) {
              final long i = total.incrementAndGet();
              if (i >= CYCLES)
                break;

              try {
                final int op = rnd.nextInt(6);
                if (i % 10000 == 0)
                  PLogManager.instance().info(this, "Operations %d/%d (thread=%d)", i, CYCLES, threadId);

                PLogManager.instance().debug(this, "Operation %d %d/%d (thread=%d)", op, i, CYCLES, threadId);

                switch (op) {
                case 0:
                case 1:
                case 2:
                case 3:
                  final int txOps = rnd.nextInt(10);
                  PLogManager.instance().debug(this, "Creating %d transactions (thread=%d)...", txOps, threadId);
                  createTransactions(database, txOps);
                  break;

                case 4:
                  PLogManager.instance().debug(this, "Deleting records (thread=%d)...", threadId);
                  deleteRecords(database, threadId);
                  break;

                case 5:
                  PLogManager.instance().debug(this, "Committing (thread=%d)...", threadId);
                  database.commit();
                  database.begin();
                  break;
                }

              } catch (Exception e) {
                if (e instanceof PConcurrentModificationException) {
                  mvccErrors.incrementAndGet();
                  total.decrementAndGet();
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

  private void deleteRecords(final PDatabase database, final int threadId) {
    final Iterator<PRecord> iter = database.iterateType("Account");

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final PRecord next = iter.next();

      if (rnd.nextInt(2) == 0) {
        database.deleteRecord(next.getIdentity());
        PLogManager.instance().debug(this, "Deleted record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }
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


