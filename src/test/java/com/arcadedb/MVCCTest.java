package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.database.async.PErrorCallback;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PVertexType;
import com.arcadedb.utility.PLogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import performance.PerformanceTest;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCTest {
  private static final int CYCLES      = 3;
  private static final int TOT_ACCOUNT = 100;
  private static final int TOT_TX      = 100;
  private static final int PARALLEL    = Runtime.getRuntime().availableProcessors();

  @Test
  public void testMVCC() {
    for (int i = 0; i < CYCLES; ++i) {
      PerformanceTest.clean();

      createSchema();

      populateDatabase();

      PLogManager.instance().info(this, "Executing " + TOT_TX + " transactions between " + TOT_ACCOUNT + " accounts");

      final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

      database.asynch().setParallelLevel(PARALLEL);

      final AtomicLong otherErrors = new AtomicLong();
      final AtomicLong mvccErrors = new AtomicLong();
      database.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {

          if (exception instanceof PConcurrentModificationException) {
            mvccErrors.incrementAndGet();
          } else {
            otherErrors.incrementAndGet();
            PLogManager.instance().error(this, "UNEXPECTED ERROR: " + exception, exception);
          }
        }
      });

      long begin = System.currentTimeMillis();

      try {
        final Random rnd = new Random();

        for (long txId = 0; txId < TOT_TX; ++txId) {
          database.asynch().transaction(new PDatabase.PTransaction() {
            @Override
            public void execute(PDatabase database) {
              Assertions.assertTrue(database.getTransaction().getModifiedPages() == 0);
              Assertions.assertNull(database.getTransaction().getPageCounter(1));

              final PModifiableDocument tx = database.newVertex("Transaction");
              tx.set("uuid", UUID.randomUUID().toString());
              tx.set("date", new Date());
              tx.set("amount", rnd.nextInt(TOT_ACCOUNT));
              tx.save();

              final PCursor<PRID> accounts = database.lookupByKey("Account", new String[] { "id" }, new Object[] { 0 });

              Assertions.assertTrue(accounts.hasNext());

              PRID account = accounts.next();

              ((PModifiableVertex) tx).newEdge("PurchasedBy", account, true, "date", new Date());
            }
          }, 0);
        }

      } finally {
        database.close();

        Assertions.assertTrue(mvccErrors.get() > 0);
        Assertions.assertEquals(0, otherErrors.get());

        System.out.println(
            "Insertion finished in " + (System.currentTimeMillis() - begin) + "ms, managed mvcc exceptions " + mvccErrors.get());
      }

      PLogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void populateDatabase() {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    try {
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(true);
      database.asynch().setCommitEvery(20000);
      database.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {
          PLogManager.instance().error(this, "ERROR: " + exception, exception);
        }
      });

      for (long row = 0; row < TOT_ACCOUNT; ++row) {
        final PModifiableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        database.asynch().createRecord(record);
      }

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

        database.getSchema().createClassIndexes("Account", new String[] { "id" }, 5000000);

        final PVertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
        txType.createProperty("uuid", String.class);
        txType.createProperty("date", Date.class);
        txType.createProperty("amount", BigDecimal.class);

        database.getSchema().createClassIndexes("Transaction", new String[] { "uuid" }, 5000000);

        final PEdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
        edgeType.createProperty("date", Date.class);

        database.commit();
      }
    } finally {
      database.close();
    }
  }
}