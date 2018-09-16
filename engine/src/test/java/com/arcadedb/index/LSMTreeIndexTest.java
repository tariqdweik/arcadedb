/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.BaseTest;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LSMTreeIndexTest extends BaseTest {
  private static final int    TOT       = 100000;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = 20000;

  @Test
  public void testGet() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          final List<Integer> results = new ArrayList<>();
          for (Index index : indexes) {
            final IndexCursor value = index.get(new Object[] { i });
            if (value.hasNext())
              results.add((Integer) ((Document) value.next().getRecord()).get("id"));
          }

          total++;
          Assertions.assertEquals(1, results.size());
          Assertions.assertEquals(i, (int) results.get(0));
        }

        Assertions.assertEquals(TOT, total);
      }
    });
  }

  @Test
  public void testRemoveKeys() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              index.remove(key);
              found++;
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);

        // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes) {
            if (!index.get(new Object[] { i }).hasNext()) {
              LogManager.instance().info(this, "FOUND KEY " + i + " -> " + index.get(new Object[] { i }));
            }

//            Assertions.assertTrue(index.get(new Object[] { i }).isEmpty(), "Found item with key " + i);
          }
        }
      }
    }, 0);

    // CHECK ALSO AFTER THE TX HAS BEEN COMMITTED
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        final Index[] indexes = database.getSchema().getIndexes();
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes) {
            Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
          }
        }
      }
    }, 0);
  }

  @Test
  public void testRemoveEntries() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              for (RID r : value)
                index.remove(key, r);
              found++;
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);

        // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes)
            Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
        }

      }
    });
  }

  @Test
  public void testRemoveEntriesMultipleTimes() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              for (RID r : value) {
                for (int k = 0; k < 10; ++k)
                  index.remove(key, r);
              }
              found++;
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);

        // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes)
            Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
        }
      }
    });
  }

  @Test
  public void testRemoveAndPutEntries() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              for (RID r : value) {
                index.remove(key, r);
                index.put(key, new RID[] { r });
                index.remove(key, r);
              }
              found++;
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);

        // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes)
            Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
        }
      }
    });
  }

  @Test
  public void testUpdateKeys() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final ResultSet resultSet = database.query("sql", "select from " + TYPE_NAME);
        for (ResultSet it = resultSet; it.hasNext(); ) {
          final Result r = it.next();

          final MutableDocument record = (MutableDocument) r.getElement().get().modify();
          record.set("id", (Integer) record.get("id") + 1000000);
          record.save();
        }

        database.commit();
        database.begin();

        final Index[] indexes = database.getSchema().getIndexes();

        // ORIGINAL KEYS SHOULD BE REMOVED
        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              found++;
              total++;
            }
          }

          Assertions.assertEquals(0, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(0, total);

        total = 0;

        // CHECK FOR NEW KEYS
        for (int i = 1000000; i < 1000000 + TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {
            final IndexCursor value = index.get(key);

            if (value.hasNext()) {
              for (RID r : value) {
                index.remove(key, r);
                found++;
              }
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);

        // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
        for (int i = 0; i < TOT; ++i) {
          for (Index index : indexes)
            Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
        }

      }
    });
  }

  @Test
  public void testPutDuplicates() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();

        for (int i = 0; i < TOT; ++i) {
          int found = 0;

          final Object[] key = new Object[] { i };

          for (Index index : indexes) {

            final IndexCursor value = index.get(key);
            if (value.hasNext()) {
              try {
                index.put(key, new RID[] { new RID(database, 10, 10) });
                database.commit();
                Assertions.fail();
              } catch (DuplicatedKeyException e) {
                // OK
              }
              database.begin();
              found++;
              total++;
            }
          }

          Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
        }

        Assertions.assertEquals(TOT, total);
      }
    });
  }

  @Test
  public void testScanIndexAscending() throws IOException {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();
        for (Index index : indexes) {
          Assertions.assertNotNull(index);

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).iterator(true);

//            LogManager.instance()
//                .info(this, "*****************************************************************************\nCURSOR BEGIN%s", iterator.dumpStats());

            Assertions.assertNotNull(iterator);

            while (iterator.hasNext()) {
              Assertions.assertNotNull(iterator.next());

              Assertions.assertNotNull(iterator.getKeys());
              Assertions.assertEquals(1, iterator.getKeys().length);

              total++;
            }

//            LogManager.instance().info(this, "*****************************************************************************\nCURSOR END total=%d %s", total,
//                iterator.dumpStats());

          } catch (IOException e) {
            Assertions.fail(e);
          }
        }

        Assertions.assertEquals(TOT, total);
      }
    });
  }

  @Test
  public void testScanIndexDescending() throws IOException {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();
        for (Index index : indexes) {
          Assertions.assertNotNull(index);

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).iterator(false);
            Assertions.assertNotNull(iterator);

            while (iterator.hasNext()) {
              Assertions.assertNotNull(iterator.next());

              Assertions.assertNotNull(iterator.getKeys());
              Assertions.assertEquals(1, iterator.getKeys().length);

              total++;
            }
          } catch (IOException e) {
            Assertions.fail(e);
          }
        }

        Assertions.assertEquals(TOT, total);
      }
    });
  }

  @Test
  public void testScanIndexAscendingPartial() throws IOException {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();
        for (Index index : indexes) {
          Assertions.assertNotNull(index);

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).iterator(true, new Object[] { 10 });

            Assertions.assertNotNull(iterator);

            while (iterator.hasNext()) {
              Assertions.assertNotNull(iterator.next());

              Assertions.assertNotNull(iterator.getKeys());
              Assertions.assertEquals(1, iterator.getKeys().length);

              total++;
            }
          } catch (IOException e) {
            Assertions.fail(e);
          }
        }

        Assertions.assertEquals(TOT - 10, total);
      }
    });
  }

  @Test
  public void testScanIndexDescendingPartial() throws IOException {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();
        for (Index index : indexes) {
          Assertions.assertNotNull(index);

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).iterator(false, new Object[] { 9 });
            Assertions.assertNotNull(iterator);

            while (iterator.hasNext()) {
              Assertions.assertNotNull(iterator.next());

              Assertions.assertNotNull(iterator.getKeys());
              Assertions.assertEquals(1, iterator.getKeys().length);

              total++;
            }
          } catch (IOException e) {
            Assertions.fail(e);
          }
        }

        Assertions.assertEquals(10, total);
      }
    });
  }

  @Test
  public void testScanIndexRange() throws IOException {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {

        int total = 0;

        final Index[] indexes = database.getSchema().getIndexes();
        for (Index index : indexes) {
          Assertions.assertNotNull(index);

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).range(new Object[] { 10 }, new Object[] { 19 });
            Assertions.assertNotNull(iterator);

            while (iterator.hasNext()) {
              Identifiable value = (Identifiable) iterator.next();

              Assertions.assertNotNull(value);

              int fieldValue = (int) ((Document) value.getRecord()).get("id");
              Assertions.assertTrue(fieldValue >= 10 && fieldValue <= 19);

              Assertions.assertNotNull(iterator.getKeys());
              Assertions.assertEquals(1, iterator.getKeys().length);

              total++;
            }
          } catch (IOException e) {
            Assertions.fail(e);
          }
        }

        Assertions.assertEquals(10, total);
      }
    });
  }

  @Test
  public void testUnique() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

    database.begin();
    final long startingWith = database.countType(TYPE_NAME, true);

    final long total = 2000;
    final int maxRetries = 100;

    final Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors() * 4];

    final AtomicLong needRetryExceptions = new AtomicLong();
    final AtomicLong duplicatedExceptions = new AtomicLong();
    final AtomicLong crossThreadsInserted = new AtomicLong();

    LogManager.instance().info(this, "%s Started with %d threads", getClass(), threads.length);

    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            int threadInserted = 0;
            for (int i = TOT; i < TOT + total; ++i) {
              boolean keyPresent = false;
              for (int retry = 0; retry < maxRetries && !keyPresent; ++retry) {

                try {
                  Thread.sleep(new Random().nextInt(10));
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  Thread.currentThread().interrupt();
                  return;
                }

                database.begin();
                try {
                  final MutableDocument v = database.newDocument(TYPE_NAME);
                  v.set("id", i);
                  v.set("name", "Jay");
                  v.set("surname", "Miner");
                  v.save();

                  database.commit();

                  threadInserted++;
                  crossThreadsInserted.incrementAndGet();

                  if (threadInserted % 1000 == 0)
                    LogManager.instance()
                        .info(this, "%s Thread %d inserted %d records with key %d (total=%d)", getClass(), Thread.currentThread().getId(), i, threadInserted,
                            crossThreadsInserted.get());

                  keyPresent = true;

                } catch (NeedRetryException e) {
                  needRetryExceptions.incrementAndGet();
                  Assertions.assertFalse(database.isTransactionActive());
                  continue;
                } catch (DuplicatedKeyException e) {
                  duplicatedExceptions.incrementAndGet();
                  keyPresent = true;
                  Assertions.assertFalse(database.isTransactionActive());
                } catch (Exception e) {
                  LogManager.instance().error(this, "%s Thread %d Generic Exception", e, getClass(), Thread.currentThread().getId());
                  Assertions.assertFalse(database.isTransactionActive());
                  return;
                }
              }

              if (!keyPresent)
                LogManager.instance()
                    .warn(this, "%s Thread %d Cannot create key %d after %d retries! (total=%d)", getClass(), Thread.currentThread().getId(), i, maxRetries,
                        crossThreadsInserted.get());

            }

            LogManager.instance().info(this, "%s Thread %d completed (inserted=%d)", getClass(), Thread.currentThread().getId(), threadInserted);

          } catch (Exception e) {
            LogManager.instance().error(this, "%s Thread %d Error", e, getClass(), Thread.currentThread().getId());
          }
        }

      });
    }

    for (int i = 0; i < threads.length; ++i)
      threads[i].start();

    for (int i = 0; i < threads.length; ++i) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LogManager.instance().info(this, "%s Completed (inserted=%d needRetryExceptions=%d duplicatedExceptions=%d)", getClass(), crossThreadsInserted.get(),
        needRetryExceptions.get(), duplicatedExceptions.get());

    if (total != crossThreadsInserted.get()) {
      LogManager.instance().info(this, "DUMP OF INSERTED RECORDS (ORDERED BY ID)");
      final ResultSet resultset = database
          .query("sql", "select id, count(*) as total from ( select from " + TYPE_NAME + " group by id ) where total > 1 order by id");
      while (resultset.hasNext())
        LogManager.instance().info(this, "- %s", resultset.next());

      LogManager.instance().info(this, "COUNT OF INSERTED RECORDS (ORDERED BY ID)");
      final Map<Integer, Integer> result = new HashMap<>();
      database.scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          final int id = (int) record.get("id");
          Integer key = result.get(id);
          if (key == null)
            result.put(id, 1);
          else
            result.put(id, key + 1);
          return true;
        }
      });

      LogManager.instance().info(this, "FOUND %d ENTRIES", result.size());

      Iterator<Map.Entry<Integer, Integer>> it = result.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Integer, Integer> next = it.next();
        if (next.getValue() > 1)
          LogManager.instance().info(this, "- %d = %d", next.getKey(), next.getValue());
      }
    }

    Assertions.assertEquals(total, crossThreadsInserted.get());
//    Assertions.assertTrue(needRetryExceptions.get() > 0);
    Assertions.assertTrue(duplicatedExceptions.get() > 0);

    Assertions.assertEquals(startingWith + total, database.countType(TYPE_NAME, true));
  }

  protected void beginTest() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        final Index[] indexes = database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save();
        }

        database.commit();
        database.begin();

        for (Index index : indexes) {
          Assertions.assertTrue(index.getStats().get("pages") > 1);
        }
      }
    });
  }
}