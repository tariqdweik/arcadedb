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
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class RandomTestSingleThread {
  private static final int CYCLES           = 1000000;
  private static final int STARTING_ACCOUNT = 100;
  private static final int PARALLEL         = Runtime.getRuntime().availableProcessors();

  private final AtomicLong otherErrors = new AtomicLong();
  private final AtomicLong mvccErrors  = new AtomicLong();
  private final Random     rnd         = new Random();

  @Test
  public void testRandom() {
    PLogManager.instance().info(this, "Executing " + CYCLES + " transactions");

    PerformanceTest.clean();
    createSchema();
    populateDatabase();

    long begin = System.currentTimeMillis();

    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {
      database.begin();

      for (int i = 0; i < CYCLES; ++i) {
        try {

          final int op = rnd.nextInt(6);

          PLogManager.instance().info(this, "Operation %d %d/%d", op, i, CYCLES);

          switch (op) {
          case 0:
          case 1:
          case 2:
            createTransactions(database);
            break;
          case 3:
            deleteRecords(database);
            break;
          case 4:
            // RANDOM PAUSE
            Thread.sleep(rnd.nextInt(100));
            break;
          case 5:
            PLogManager.instance().info(this, "Comitting...");
            database.commit();
            database.begin();
            break;
          }

        } catch (Exception e) {
          if (e instanceof PConcurrentModificationException) {
            mvccErrors.incrementAndGet();
          } else {
            otherErrors.incrementAndGet();
            PLogManager.instance().error(this, "UNEXPECTED ERROR: " + e, e);
          }
        }
      }

      database.commit();

    } finally {
      database.close();

      System.out.println("Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get()
          + " otherExceptions=" + otherErrors.get());
    }

    PLogManager.instance().flush();
    System.out.flush();
    System.out.println("----------------");
  }

  private void createTransactions(PDatabase database) {
    final int txOps = rnd.nextInt(100);

    PLogManager.instance().info(this, "Creating %d transactions...", txOps);

    for (long txId = 0; txId < txOps; ++txId) {
      final PModifiableDocument tx = database.newVertex("Transaction");
      tx.set("uuid", UUID.randomUUID().toString());
      tx.set("date", new Date());
      tx.set("amount", rnd.nextInt(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private void deleteRecords(PDatabase database) {
    PLogManager.instance().info(this, "Deleting records...");

    final Iterator<PRecord> iter = database.iterateType("Account");

    while (iter.hasNext() && rnd.nextInt(10) != 0) {
      final PRecord next = iter.next();

      if (rnd.nextInt(2) == 0) {
        database.deleteRecord(next.getIdentity());
        PLogManager.instance().info(this, "Deleted record %s", next.getIdentity());
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

        database.getSchema().createClassIndexes("Account", new String[] { "id" });

        final PVertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
        txType.createProperty("uuid", String.class);
        txType.createProperty("date", Date.class);
        txType.createProperty("amount", BigDecimal.class);

        database.getSchema().createClassIndexes("Transaction", new String[] { "uuid" });

        final PEdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
        edgeType.createProperty("date", Date.class);

        database.commit();
      }
    } finally {
      database.close();
    }
  }
}