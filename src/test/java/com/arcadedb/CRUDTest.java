package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PDatabaseChecker;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class CRUDTest {
  private static final int    TOT     = PBucket.DEF_PAGE_SIZE * 2;
  private static final String DB_PATH = "target/database/testdb";

  @BeforeEach
  public void populate() {
    PFileUtils.deleteRecursively(new File(DB_PATH));
    PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    createAll(db);
    db.close();
  }

  @AfterEach
  public void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testUpdate() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      db.scanType("V", true, new PDocumentCallback() {
        @Override
        public boolean onRecord(PDocument record) {
          final PModifiableDocument document = (PModifiableDocument) record.modify();
          document.set("update", true);
          document.set("largeField", "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
          document.save();
          return true;
        }
      });

      db.commit();

      Assertions.assertEquals(TOT, db.countType("V", true));

      db.scanType("V", true, new PDocumentCallback() {
        @Override
        public boolean onRecord(PDocument record) {
          Assertions.assertEquals(true, record.get("update"));
          Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField"));
          return true;
        }
      });

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void testMultiUpdatesOverlap() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    try {

      for (int i = 0; i < 10; ++i) {
        db.begin();
        updateAll(db, "largeField" + i);

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();

        Assertions.assertEquals(TOT, db.countType("V", true));

        PLogManager.instance().info(this, "Completed %d cycle of updates", i);
      }

      db.scanType("V", true, new PDocumentCallback() {
        @Override
        public boolean onRecord(PDocument record) {
          Assertions.assertEquals(true, record.get("update"));

          for (int i = 0; i < 10; ++i)
            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + i));

          return true;
        }
      });

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void testMultiUpdatesAndDeleteOverlap() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {

      for (int i = 0; i < 10; ++i) {
        final int counter = i;

        db.begin();

        updateAll(db, "largeField" + i);
        Assertions.assertEquals(TOT, db.countType("V", true));

        db.commit();
        db.begin();

        Assertions.assertEquals(TOT, db.countType("V", true));

        db.scanType("V", true, new PDocumentCallback() {
          @Override
          public boolean onRecord(PDocument record) {
            Assertions.assertEquals(true, record.get("update"));

            Assertions.assertEquals("This is a large field to force the page overlap at some point", record.get("largeField" + counter));

            return true;
          }
        });

        deleteAll(db);

        Assertions.assertEquals(0, db.countType("V", true));

        db.commit();

        Assertions.assertEquals(0, db.countType("V", true));

        PLogManager.instance().info(this, "Completed %d cycle of updates+delete", i);

        createAll(db);

        Assertions.assertEquals(TOT, db.countType("V", true));
      }

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }

  private void createAll(PDatabase db) {
    db.transaction(new PDatabase.PTransaction() {
      @Override
      public void execute(PDatabase database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");

        for (int i = 0; i < TOT; ++i) {
          final PModifiableDocument v = database.newDocument("V");
          v.set("id", i);
          v.set("name", "V" + i);
          v.save();
        }
      }
    });
  }

  private void updateAll(PDatabase db, String largeField) {
    db.scanType("V", true, new PDocumentCallback() {
      @Override
      public boolean onRecord(PDocument record) {
        final PModifiableDocument document = (PModifiableDocument) record.modify();
        document.set("update", true);
        document.set(largeField, "This is a large field to force the page overlap at some point"); // FORCE THE PAGE OVERLAP
        document.save();
        return true;
      }
    });
  }

  private void deleteAll(PDatabase db) {
    db.scanType("V", true, new PDocumentCallback() {
      @Override
      public boolean onRecord(PDocument record) {
        db.deleteRecord(record);
        return true;
      }
    });
  }

}