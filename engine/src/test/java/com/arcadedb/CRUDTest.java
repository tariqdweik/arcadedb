/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class CRUDTest {
  private static final int    TOT     = Bucket.DEF_PAGE_SIZE * 2;
  private static final String DB_PATH = "target/database/testdb";

  @BeforeEach
  public void populate() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    createAll(db);
    db.close();
  }

  @AfterEach
  public void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testUpdate() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          final ModifiableDocument document = (ModifiableDocument) record.modify();
          document.set("update", true);
          document.set("largeField", "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
          document.save();
          return true;
        }
      });

      db.commit();

      Assertions.assertEquals(TOT, db.countType("V", true));

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          Assertions.assertEquals(true, record.get("update"));
          Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField"));
          return true;
        }
      });

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void testMultiUpdatesOverlap() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();

    try {

      for (int i = 0; i < 10; ++i) {
        db.begin();
        updateAll(db, "largeField" + i);

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();

        Assertions.assertEquals(TOT, db.countType("V", true));

        LogManager.instance().info(this, "Completed %d cycle of updates", i);
      }

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          Assertions.assertEquals(true, record.get("update"));

          for (int i = 0; i < 10; ++i)
            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + i));

          return true;
        }
      });

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void testMultiUpdatesAndDeleteOverlap() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    try {

      for (int i = 0; i < 10; ++i) {
        final int counter = i;

        db.begin();

        updateAll(db, "largeField" + i);
        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();
        db.begin();

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.scanType("V", true, new DocumentCallback() {
          @Override
          public boolean onRecord(Document record) {
            Assertions.assertEquals(true, record.get("update"));

            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + counter));

            return true;
          }
        });

        deleteAll(db);

        Assertions.assertEquals(0, db.countType("V", true));

        db.commit();

        Assertions.assertEquals(0, db.countType("V", true));

        LogManager.instance().info(this, "Completed %d cycle of updates+delete", i);

        createAll(db);

        Assertions.assertEquals(TOT, db.countType("V", true));
      }

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  private void createAll(Database db) {
    db.transaction(new Database.PTransaction() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final ModifiableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "V" + i);
          v.save();
        }
      }
    });
  }

  private void updateAll(Database db, String largeField) {
    db.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        final ModifiableDocument document = (ModifiableDocument) record.modify();
        document.set("update", true);
        document.set(largeField, "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
        document.save();
        return true;
      }
    });
  }

  private void deleteAll(Database db) {
    db.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        db.deleteRecord(record);
        return true;
      }
    });
  }

}