/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CRUDTest extends BaseTest {
  private static final int TOT = Bucket.DEF_PAGE_SIZE * 2;

  @Override
  protected void beginTest() {
    createAll();
  }

  @Test
  public void testUpdate() {
    final Database db = database;
    db.begin();
    try {

      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          final MutableDocument document = (MutableDocument) record.modify();
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
      new DatabaseChecker().check(database);
    }
  }

  @Test
  public void testMultiUpdatesOverlap() {
    final Database db = database;

    try {

      for (int i = 0; i < 10; ++i) {
        db.begin();
        updateAll("largeField" + i);

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
      new DatabaseChecker().check(database);
    }
  }

  @Test
  public void testMultiUpdatesAndDeleteOverlap() {
    final Database db = database;
    try {

      for (int i = 0; i < 10; ++i) {
        final int counter = i;

        db.begin();

        updateAll("largeField" + i);
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

        deleteAll();

        Assertions.assertEquals(0, db.countType("V", true));

        db.commit();

        Assertions.assertEquals(0, db.countType("V", true));

        LogManager.instance().info(this, "Completed %d cycle of updates+delete", i);

        createAll();

        Assertions.assertEquals(TOT, db.countType("V", true));
      }

    } finally {
      new DatabaseChecker().check(database);
    }
  }

  private void createAll() {
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "V" + i);
          v.save();
        }
      }
    });
  }

  private void updateAll(String largeField) {
    database.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        final MutableDocument document = (MutableDocument) record.modify();
        document.set("update", true);
        document.set(largeField, "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
        document.save();
        return true;
      }
    });
  }

  private void deleteAll() {
    database.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        database.deleteRecord(record);
        return true;
      }
    });
  }

}