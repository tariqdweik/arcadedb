/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionTypeTest extends BaseTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, new DocumentCallback() {
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

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, new DocumentCallback() {
      @Override
      public boolean onRecord(final Document record) {
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
  public void testLookupAllRecordsByKey() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    for (int i = 0; i < TOT; i++) {
      final Cursor<RID> result = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { i });
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

    database.commit();

    Assertions.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, new DocumentCallback() {
      @Override
      public boolean onRecord(final Document record) {
        database.deleteRecord(record);
        total.incrementAndGet();
        return true;
      }
    });

    database.commit();

    Assertions.assertEquals(TOT, total.get());

    database.begin();

    Assertions.assertEquals(0, database.countType(TYPE_NAME, true));

    // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
    final Index[] indexes = database.getSchema().getIndexes();
    for (int i = 0; i < TOT; ++i) {
      for (Index index : indexes)
        Assertions.assertFalse(index.get(new Object[] { i }).hasNext(), "Found item with key " + i);
    }

    beginTest();

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        Assertions.assertEquals(TOT, database.countType(TYPE_NAME, true));
      }
    });
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    Assertions.assertThrows(DatabaseIsReadOnlyException.class, () -> {

      database.begin();

      database.scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(final Document record) {
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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType(TYPE_NAME)) {
          final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
          type.createProperty("id", Integer.class);
          database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" });
        }

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save();
        }
      }
    });
  }
}