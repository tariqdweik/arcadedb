/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionTypeTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final String DB_PATH   = "target/database/testdb";

  @BeforeEach
  public void populate() {
    populate(TOT);
  }

  @AfterEach
  public void drop() {
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
      db.scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(final Document record) {
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<String>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
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
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanType(TYPE_NAME,true,  new DocumentCallback() {
        @Override
        public boolean onRecord(final Document record) {
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
  public void testLookupAllRecordsByKey() {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      for (int i = 0; i < TOT; i++) {
        final Cursor<RID> result = db.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { i });
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        final Document record2 = (Document) result.next().getRecord();

        Assertions.assertEquals(i, record2.get("id"));

        Set<String> prop = new HashSet<String>();
        for (String p : record2.getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(record2.getPropertyNames().size(), 3);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
      }

      db.commit();

    } finally {
      db.close();
    }

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      db.scanType(TYPE_NAME,true,  new DocumentCallback() {
        @Override
        public boolean onRecord(final Document record) {
          db.deleteRecord(record);
          total.incrementAndGet();
          return true;
        }
      });

      db.commit();

    } finally {
      db.close();
    }

    Assertions.assertEquals(TOT, total.get());

    populate();

    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db2.begin();
    try {
      Assertions.assertEquals(TOT, db2.countType(TYPE_NAME, true));
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
        db.scanType(TYPE_NAME,true,  new DocumentCallback() {
          @Override
          public boolean onRecord(final Document record) {
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

  private void populate(final int total) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final PDocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" });

        for (int i = 0; i < total; ++i) {
          final ModifiableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save();
        }
      }
    });
  }
}