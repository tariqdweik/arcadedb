package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.engine.PFile;
import com.arcadedb.engine.PIndex;
import com.arcadedb.engine.PIndexCursor;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PFileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class IndexTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final String DB_PATH   = "target/database/testdb";

  @BeforeAll
  public static void populate() {
    populate(TOT);
  }

  @AfterAll
  public static void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testScanIndexAscending() throws IOException {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexCursor iterator = index.iterator(true);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexCursor iterator = index.iterator(false);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexCursor iterator = index.iterator(true, new Object[] { 10 });
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexCursor iterator = index.iterator(false, new Object[] { 9 });
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexCursor iterator = index.range(new Object[] { 10 }, new Object[] { 19 });
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

          total++;
        }
      }

      Assertions.assertEquals(10, total);

    } finally {
      db.close();
    }
  }

  private static void populate(final int total) {
    PFileUtils.deleteRecursively(new File(DB_PATH));

    new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final PDocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" }, 20000);

        for (int i = 0; i < total; ++i) {
          final PModifiableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save();
        }
      }
    });
  }
}