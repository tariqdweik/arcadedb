/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionBucketTest extends BaseTest {
  private static final int TOT = 10000;

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanBucket("V_0", new RecordCallback() {
      @Override
      public boolean onRecord(final Record record) {
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<String>();
        for (String p : ((Document) record).getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(3, ((Document) record).getPropertyNames().size(), 9);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
        return true;
      }
    });

    Assertions.assertEquals(TOT, total.get());

    database.commit();
  }

  @Test
  public void testIterator() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    Iterator<Record> iterator = database.iterateBucket("V_0");

    while (iterator.hasNext()) {
      Document record = (Document) iterator.next();
      Assertions.assertNotNull(record);

      Set<String> prop = new HashSet<String>();
      for (String p : record.getPropertyNames())
        prop.add(p);

      Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
      Assertions.assertTrue(prop.contains("id"));
      Assertions.assertTrue(prop.contains("name"));
      Assertions.assertTrue(prop.contains("surname"));

      total.incrementAndGet();

    }

    Assertions.assertEquals(TOT, total.get());

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanBucket("V_0", new RecordCallback() {
      @Override
      public boolean onRecord(final Record record) {
        final Document record2 = (Document) database.lookupByRID(record.getIdentity(), false);
        Assertions.assertNotNull(record2);
        Assertions.assertEquals(record, record2);

        Set<String> prop = new HashSet<String>();
        for (String p : record2.getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(record2.getPropertyNames().size(), 3);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
        return true;
      }
    });

    database.commit();

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();
    try {
      database.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          database.deleteRecord(record);
          total.incrementAndGet();
          return true;
        }
      });

      database.commit();

    } finally {
      Assertions.assertEquals(0, database.countBucket("V_0"));
    }

    Assertions.assertEquals(TOT, total.get());

    beginTest();

    Assertions.assertEquals(TOT, database.countBucket("V_0"));
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    Assertions.assertThrows(DatabaseIsReadOnlyException.class, () -> {
      database.begin();

      database.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          database.deleteRecord(record);
          return true;
        }
      });

      database.commit();
    });

    reopenDatabase();
  }

  @Override
  protected void beginTest() {
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save("V_0");
        }
      }
    });
  }
}