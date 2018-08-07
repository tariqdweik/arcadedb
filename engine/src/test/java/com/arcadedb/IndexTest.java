/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class IndexTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final String DB_PATH   = "target/database/testdb";

  @BeforeEach
  public void populate() {
    populate(TOT);
  }

  @AfterEach
  public void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.drop();
  }

  @Test
  public void testGet() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        final List<Integer> results = new ArrayList<>();
        for (Index index : indexes) {
          final Set<RID> value = index.get(new Object[] { i });
          if (!value.isEmpty())
            results.add((Integer) ((Document) value.iterator().next().getRecord()).get("id"));
        }

        total++;
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(i, (int) results.get(0));
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testRemoveKeys() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (Index index : indexes) {
          final Set<RID> value = index.get(key);
          if (!value.isEmpty()) {
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
        for (Index index : indexes)
          Assertions.assertTrue(index.get(new Object[] { i }).isEmpty(), "Found item with key " + i);
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testRemoveEntries() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (Index index : indexes) {
          final Set<RID> value = index.get(key);
          if (!value.isEmpty()) {
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
          Assertions.assertTrue(index.get(new Object[] { i }).isEmpty(), "Found item with key " + i);
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testRemoveEntriesMultipleTimes() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (Index index : indexes) {
          final Set<RID> value = index.get(key);
          if (!value.isEmpty()) {
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
          Assertions.assertTrue(index.get(new Object[] { i }).isEmpty(), "Found item with key " + i);
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testRemoveAndPutEntries() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (Index index : indexes) {
          final Set<RID> value = index.get(key);
          if (!value.isEmpty()) {
            for (RID r : value) {
              index.remove(key, r);
              index.put(key, r);
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
          Assertions.assertTrue(index.get(new Object[] { i }).isEmpty(), "Found item with key " + i);
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testPutDuplicates() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (Index index : indexes) {

          final Set<RID> value = index.get(key);
          if (!value.isEmpty()) {
            try {
              index.put(key, new RID(db, 10, 10));
              Assertions.fail();
            } catch (DuplicatedKeyException e) {
              // OK
            }
            found++;
            total++;
          }
        }

        Assertions.assertEquals(1, found, "Key '" + Arrays.toString(key) + "' found " + found + " times");
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexAscending() throws IOException {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();
      for (Index index : indexes) {
        Assertions.assertNotNull(index);

        final IndexCursor iterator = index.iterator(true);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          Assertions.assertNotNull(iterator.next());

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          total++;
        }
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexDescending() throws IOException {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();
      for (Index index : indexes) {
        Assertions.assertNotNull(index);

        final IndexCursor iterator = index.iterator(false);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          Assertions.assertNotNull(iterator.next());

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          total++;
        }
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexAscendingPartial() throws IOException {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();
      for (Index index : indexes) {
        Assertions.assertNotNull(index);

        final IndexCursor iterator = index.iterator(true, new Object[] { 10 });
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          Assertions.assertNotNull(iterator.next());

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          total++;
        }
      }

      Assertions.assertEquals(TOT - 10, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexDescendingPartial() throws IOException {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();
      for (Index index : indexes) {
        Assertions.assertNotNull(index);

        final IndexCursor iterator = index.iterator(false, new Object[] { 9 });
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          Assertions.assertNotNull(iterator.next());

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          total++;
        }
      }

      Assertions.assertEquals(10, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexRange() throws IOException {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      int total = 0;

      final Index[] indexes = db.getSchema().getIndexes();
      for (Index index : indexes) {
        Assertions.assertNotNull(index);

        final IndexCursor iterator = index.range(new Object[] { 10 }, new Object[] { 19 });
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
      }

      Assertions.assertEquals(10, total);

    } finally {
      db.close();
    }
  }

  private static void populate(final int total) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.DatabaseOperation() {
      @Override
      public void execute(Database database) {
        Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        final Index[] indexes = database.getSchema().createClassIndexes(true, TYPE_NAME, new String[] { "id" }, 1000);

        for (int i = 0; i < total; ++i) {
          final ModifiableDocument v = database.newDocument(TYPE_NAME);
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