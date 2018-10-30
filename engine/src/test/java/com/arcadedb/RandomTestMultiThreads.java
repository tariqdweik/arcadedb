/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RandomTestMultiThreads extends BaseTest {
  private static final int CYCLES           = 10000;
  private static final int STARTING_ACCOUNT = 10000;
  private static final int PARALLEL         = Runtime.getRuntime().availableProcessors();
  private static final int WORKERS          = Runtime.getRuntime().availableProcessors() * 8;

  private final AtomicLong                     total                   = new AtomicLong();
  private final AtomicLong                     totalTransactionRecords = new AtomicLong();
  private final AtomicLong                     mvccErrors              = new AtomicLong();
  private final Random                         rnd                     = new Random();
  private final AtomicLong                     uuid                    = new AtomicLong();
  private final List<Pair<Integer, Exception>> otherErrors             = Collections.synchronizedList(new ArrayList<>());

  @Test
  public void testRandom() {
    LogManager.instance().info(this, "Executing " + CYCLES + " transactions with %d workers", WORKERS);

    createSchema();
    populateDatabase();

    long begin = System.currentTimeMillis();

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
                final int op = getRandom(100);
                if (i % 5000 == 0)
                  LogManager.instance()
                      .info(this, "Operations %d/%d totalTransactionInCurrentTx=%d totalTransactions=%d (thread=%d)", i, CYCLES, totalTransactionInCurrentTx,
                          totalTransactionRecords.get(), threadId);

                LogManager.instance().debug(this, "Operation %d %d/%d (thread=%d)", op, i, CYCLES, threadId);

                if (op >= 0 && op <= 19) {
                  final int txOps = getRandom(10);
                  LogManager.instance().debug(this, "Creating %d transactions (thread=%d)...", txOps, threadId);

                  createTransactions(database, txOps);
                  totalTransactionInCurrentTx += txOps;

                } else if (op >= 20 && op <= 39) {
                  LogManager.instance().debug(this, "Querying Account by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":id", getRandom(10000) + 1);

                  final ResultSet result = database.command("SQL", "select from Account where id = :id", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toString();
                  }

                } else if (op >= 40 && op <= 59) {
                  LogManager.instance().debug(this, "Querying Transaction by index records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":uuid", getRandom((int) (totalTransactionRecords.get() + 1)) + 1);

                  final ResultSet result = database.command("SQL", "select from Transaction where uuid = :uuid", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toString();
                  }
                } else if (op >= 60 && op <= 64) {
                  LogManager.instance().debug(this, "Scanning Account records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", getRandom(100) + 1);

                  final ResultSet result = database.command("SQL", "select from Account limit :limit", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toString();
                  }

                } else if (op >= 65 && op <= 69) {
                  LogManager.instance().debug(this, "Scanning Transaction records (thread=%d)...", threadId);

                  final Map<String, Object> map = new HashMap<>();
                  map.put(":limit", getRandom((int) totalTransactionRecords.get() + 1) + 1);

                  final ResultSet result = database.command("SQL", "select from Transaction limit :limit", map);
                  while (result.hasNext()) {
                    final Result record = result.next();
                    record.toString();
                  }

                } else if (op >= 70 && op <= 74) {
                  LogManager.instance().debug(this, "Deleting records (thread=%d)...", threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op >= 75 && op <= 84) {

                  LogManager.instance().debug(this, "Committing (thread=%d)...", threadId);
                  database.commit();

                  totalTransactionRecords.addAndGet(totalTransactionInCurrentTx);
                  totalTransactionInCurrentTx = 0;

                  database.begin();
                } else if (op >= 85 && op <= 94) {

                  LogManager.instance().debug(this, "Updating records (thread=%d)...", threadId);

                  updateRecords(database, threadId);
                } else if (op >= 95 && op <= 95) {
                  LogManager.instance().debug(this, "Counting Transaction records (thread=%d)...", threadId);

                  final long newCounter = database.countType("Transaction", true);

                  if (getRandom(50) == 0)
                    LogManager.instance()
                        .info(this, "Found %d Transaction records, ram counter=%d (thread=%d)...", newCounter, totalTransactionRecords.get(), threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);

                } else if (op >= 96 && op <= 96) {
                  LogManager.instance().debug(this, "Counting account records (thread=%d)...", threadId);

                  final long newCounter = database.countType("Account", true);

                  if (getRandom(50) == 0)
                    LogManager.instance().info(this, "Found %d Account records (thread=%d)...", newCounter, threadId);

                  totalTransactionInCurrentTx -= deleteRecords(database, threadId);
                } else if (op >= 97 && op <= 99) {
                  //JUST WAIT
                  final long ms = getRandom(299) + 1;
                  LogManager.instance().debug(this, "Sleeping %d ms (thread=%d)...", ms, threadId);
                  Thread.sleep(ms);
                }

              } catch (Exception e) {
                if (e instanceof ConcurrentModificationException) {
                  mvccErrors.incrementAndGet();
                  total.decrementAndGet();
                  totalTransactionInCurrentTx = 0;
                } else {
                  otherErrors.add(new Pair<>(threadId, e));
                  LogManager.instance().error(this, "UNEXPECTED ERROR: " + e, e);
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

      LogManager.instance().flush();
      System.out.flush();
      System.out.println("----------------");

      for (int i = 0; i < WORKERS; ++i) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }
      }

    } finally {
      new DatabaseChecker().check(database);

      System.out.println(
          "Test finished in " + (System.currentTimeMillis() - begin) + "ms, mvccExceptions=" + mvccErrors.get() + " otherExceptions=" + otherErrors.size());

      for (Pair<Integer, Exception> entry : otherErrors) {
        System.out.println(" = threadId=" + entry.getFirst() + " exception=" + entry.getSecond());
      }
    }
  }

  private void createTransactions(final Database database, final int txOps) {
    for (long txId = 0; txId < txOps; ++txId) {
      final MutableDocument tx = database.newVertex("Transaction");
      tx.set("uuid", "" + uuid.getAndIncrement());
      tx.set("date", new Date());
      tx.set("amount", getRandom(STARTING_ACCOUNT));
      tx.save();
    }
  }

  private int updateRecords(final Database database, final int threadId) {
    if (totalTransactionRecords.get() == 0)
      return 0;

    final Iterator<Record> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = getRandom(((int) totalTransactionRecords.get() + 1) / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int updated = 0;

    while (iter.hasNext() && getRandom(10) != 0) {
      final Record next = iter.next();

      if (getRandom(2) == 0) {
        try {
          final MutableDocument doc = (MutableDocument) next.modify();

          Integer val = (Integer) doc.get("updated");
          if (val == null)
            val = 0;
          doc.set("updated", val + 1);

          if (getRandom(2) == 1)
            doc.set("longFieldUpdated", "This is a long field to test the break of pages");

          doc.save();

          updated++;

        } catch (RecordNotFoundException e) {
          // OK
        }
        LogManager.instance().debug(this, "Updated record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return updated;
  }

  private int deleteRecords(final Database database, final int threadId) {
    if (totalTransactionRecords.get() == 0)
      return 0;

    final Iterator<Record> iter = database.iterateType("Transaction", true);

    // JUMP A RANDOM NUMBER OF RECORD
    final int jump = getRandom(((int) totalTransactionRecords.get() + 1) / 2);
    for (int i = 0; i < jump && iter.hasNext(); ++i)
      iter.next();

    int deleted = 0;

    while (iter.hasNext() && getRandom(20) != 0) {
      final Record next = iter.next();

      if (getRandom(6) != 0) {
        try {
          database.deleteRecord(next);
          deleted++;
        } catch (RecordNotFoundException e) {
          // OK
        }
        LogManager.instance().debug(this, "Deleted record %s (threadId=%d)", next.getIdentity(), threadId);
      }
    }

    return deleted;
  }

  private int getRandom(int bound) {
    if (bound < 1) {
      LogManager.instance().info(this, "Non positive bound: " + bound);
      bound = 1;
    }
    return rnd.nextInt(bound);
  }

  private void populateDatabase() {

    long begin = System.currentTimeMillis();

    database.begin();

    try {
      for (long row = 0; row < STARTING_ACCOUNT; ++row) {
        final MutableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        record.save();
      }

      database.commit();

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

      database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, true, "Account", new String[] { "id" }, 500000);

      final VertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
      txType.createProperty("uuid", String.class);
      txType.createProperty("date", Date.class);
      txType.createProperty("amount", BigDecimal.class);
      txType.createProperty("updated", Boolean.class);
      txType.createProperty("longFieldUpdated", String.class);

      database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, true, "Transaction", new String[] { "uuid" }, 500000);

      final EdgeType edgeType = database.getSchema().createEdgeType("PurchasedBy", PARALLEL);
      edgeType.createProperty("date", Date.class);

      database.commit();
    }
  }
}


