/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionBucketTest {
  private static final int    TOT     = 10000;
  private static final String DB_PATH = "target/database/testdb";

  @BeforeAll
  public static void populate() {
    populate(TOT);
  }

  @AfterAll
  public static void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanBucket("V_0", new RecordCallback() {
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

      db.commit();

    } finally {
      db.close();
    }
  }

  @Test
  public void testIterator() {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      Iterator<Record> iterator = db.iterateBucket("V_0");

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

      db.commit();

    } finally {
      db.close();
    }
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          final Document record2 = (Document) db.lookupByRID(record.getIdentity(), false);
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

      db.commit();

    } finally {
      db.close();
    }

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace()  {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      db.scanBucket("V_0", new RecordCallback() {
        @Override
        public boolean onRecord(final Record record) {
          db.deleteRecord(record);
          total.incrementAndGet();
          return true;
        }
      });

      db.commit();

    } finally {
      Assertions.assertEquals(0, db.countBucket("V_0"));

      db.close();
    }

    Assertions.assertEquals(TOT, total.get());

    populate();

    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db2.begin();
    try {
      Assertions.assertEquals(TOT, db2.countBucket("V_0"));
      db2.commit();
    } finally {
      db2.close();
    }
  }

  @Test
  public void testDeleteFail() {

    Assertions.assertThrows(DatabaseIsReadOnlyException.class, () -> {
      final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
      db.begin();
      try {
        db.scanBucket("V_0", new RecordCallback() {
          @Override
          public boolean onRecord(final Record record) {
            db.deleteRecord(record);
            return true;
          }
        });

        db.commit();

      } finally {
        db.close();
      }
    });
  }

  private static void populate(final int total) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < total; ++i) {
          final ModifiableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save("V_0");
        }
      }
    });
  }
}