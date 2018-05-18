/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.LogManager;
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

      LogManager.instance().info(this, "Executing " + TOT_TX + " transactions between " + TOT_ACCOUNT + " accounts");

      final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).acquire();

      database.asynch().setParallelLevel(PARALLEL);

      final AtomicLong otherErrors = new AtomicLong();
      final AtomicLong mvccErrors = new AtomicLong();
      database.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {

          if (exception instanceof ConcurrentModificationException) {
            mvccErrors.incrementAndGet();
          } else {
            otherErrors.incrementAndGet();
            LogManager.instance().error(this, "UNEXPECTED ERROR: " + exception, exception);
          }
        }
      });

      long begin = System.currentTimeMillis();

      try {
        final Random rnd = new Random();

        for (long txId = 0; txId < TOT_TX; ++txId) {
          database.asynch().transaction(new Database.PTransaction() {
            @Override
            public void execute(Database database) {
              Assertions.assertTrue(database.getTransaction().getModifiedPages() == 0);
              Assertions.assertNull(database.getTransaction().getPageCounter(1));

              final ModifiableDocument tx = database.newVertex("Transaction");
              tx.set("uuid", UUID.randomUUID().toString());
              tx.set("date", new Date());
              tx.set("amount", rnd.nextInt(TOT_ACCOUNT));
              tx.save();

              final Cursor<RID> accounts = database.lookupByKey("Account", new String[] { "id" }, new Object[] { 0 });

              Assertions.assertTrue(accounts.hasNext());

              RID account = accounts.next();

              ((ModifiableVertex) tx).newEdge("PurchasedBy", account, true, "date", new Date());
            }
          }, 0);
        }

      } finally {
        new DatabaseChecker().check(database);

        database.close();

        Assertions.assertTrue(mvccErrors.get() > 0);
        Assertions.assertEquals(0, otherErrors.get());

        System.out.println(
            "Insertion finished in " + (System.currentTimeMillis() - begin) + "ms, managed mvcc exceptions " + mvccErrors.get());
      }

      LogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");
    }
  }

  private void populateDatabase() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    try {
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(true);
      database.asynch().setCommitEvery(20000);
      database.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          LogManager.instance().error(this, "ERROR: " + exception, exception);
        }
      });

      for (long row = 0; row < TOT_ACCOUNT; ++row) {
        final ModifiableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        database.asynch().createRecord(record);
      }

    } finally {
      database.close();
      LogManager.instance().info(this, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType("Account")) {
        database.begin();

        final VertexType accountType = database.getSchema().createVertexType("Account", PARALLEL);
        accountType.createProperty("id", Long.class);
        accountType.createProperty("name", String.class);
        accountType.createProperty("surname", String.class);
        accountType.createProperty("registered", Date.class);

        database.getSchema().createClassIndexes(true, "Account", new String[] { "id" }, 5000000);

        final VertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
        txType.createProperty("uuid", String.class);
        txType.createProperty("date", Date.class);
        txType.createProperty("amount", BigDecimal.class);

        database.getSchema().createClassIndexes(true, "Transaction", new String[] { "uuid" }, 5000000);

        final EdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
        edgeType.createProperty("date", Date.class);

        database.commit();
      }
    } finally {
      database.close();
    }
  }
}