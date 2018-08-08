/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Cursor;
import com.arcadedb.database.Database;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCTest extends BaseTest {
  private static final int CYCLES      = 3;
  private static final int TOT_ACCOUNT = 100;
  private static final int TOT_TX      = 100;
  private static final int PARALLEL    = Runtime.getRuntime().availableProcessors();

  @Test
  public void testMVCC() {
    for (int i = 0; i < CYCLES; ++i) {
      createSchema();

      populateDatabase();

      LogManager.instance().info(this, "Executing " + TOT_TX + " transactions between " + TOT_ACCOUNT + " accounts");

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
          database.asynch().transaction(new Database.Transaction() {
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

        database.asynch().waitCompletion();

      } finally {
        new DatabaseChecker().check(database);

        Assertions.assertTrue(mvccErrors.get() > 0);
        Assertions.assertEquals(0, otherErrors.get());

        System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms, managed mvcc exceptions " + mvccErrors.get());
      }

      LogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");
    }
  }

  private void populateDatabase() {

    long begin = System.currentTimeMillis();

    try {
      database.transaction(new Database.Transaction() {
        @Override
        public void execute(Database database) {
          for (long row = 0; row < TOT_ACCOUNT; ++row) {
            final ModifiableDocument record = database.newVertex("Account");
            record.set("id", row);
            record.set("name", "Luca" + row);
            record.set("surname", "Skywalker" + row);
            record.set("registered", new Date());
            record.save();
          }
        }
      });

    } finally {
      LogManager.instance().info(this, "Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
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
  }
}