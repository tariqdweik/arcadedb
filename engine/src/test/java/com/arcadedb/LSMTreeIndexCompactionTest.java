/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * This test stresses the index compaction by forcing using only 1MB of RAM for compaction causing multiple page compacted index.
 * @author Luca
 */
public class LSMTreeIndexCompactionTest extends BaseTest {
  private static final int    TOT               = 200_000;
  private static final int    INDEX_PAGE_SIZE   = 64 * 1024; // 128K
  private static final int    COMPACTION_RAM_MB = 1; // 1MB
  private static final int    PARALLEL          = 4;
  private static final String TYPE_NAME         = "Device";

  @Test
  public void testCompaction() throws IOException {
    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);

      insertData();
      checkLookups();
      compaction();
      checkLookups();

    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(300);

    }
  }

  private void compaction() {
    for (Index index : database.getSchema().getIndexes()) {
      try {
        Assertions.assertTrue(index.compact());
      } catch (IOException e) {
        Assertions.fail(e);
      }
    }
  }

  private void insertData() {
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType(TYPE_NAME)) {
          DocumentType v = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

          v.createProperty("id", String.class);
          v.createProperty("number", String.class);
          v.createProperty("relativeName", String.class);

          v.createProperty("Name", String.class);

          database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "id" }, INDEX_PAGE_SIZE);
          database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "number" }, INDEX_PAGE_SIZE);
          database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "relativeName" }, INDEX_PAGE_SIZE);
        }
      }
    });

    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database database) {
        long begin = System.currentTimeMillis();
        try {

          database.setReadYourWrites(false);
          database.asynch().setCommitEvery(50000);
          database.asynch().setParallelLevel(PARALLEL);
          database.asynch().setTransactionUseWAL(true);
          database.asynch().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);

          database.asynch().onError(new ErrorCallback() {
            @Override
            public void call(Exception exception) {
              System.out.println("ERROR: " + exception);
              exception.printStackTrace();
            }
          });

          final int totalToInsert = TOT;
          final long startTimer = System.currentTimeMillis();
          long lastLap = startTimer;
          long lastLapCounter = 0;

          long counter = 0;
          for (; counter < totalToInsert; ++counter) {
            final MutableDocument v = database.newDocument("Device");

            final String randomString = "" + counter;

            v.set("id", randomString); // INDEXED
            v.set("number", "" + counter); // INDEXED
            v.set("relativeName", "/shelf=" + counter + "/slot=1"); // INDEXED

            v.set("Name", "1" + counter);

            database.asynch().createRecord(v);

            if (counter % 1000 == 0) {
              if (System.currentTimeMillis() - lastLap > 1000) {
                LogManager.instance().info(this, "TEST: - Progress %d/%d (%d records/sec)", counter, totalToInsert, counter - lastLapCounter);
                lastLap = System.currentTimeMillis();
                lastLapCounter = counter;
              }
            }
          }

          System.out.println("Inserted " + counter + " elements in " + (System.currentTimeMillis() - begin) + "ms");

        } finally {
          System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
        }
      }
    });
  }

  private void checkLookups() {
    reopenDatabaseInReadOnlyMode();

    long begin = System.currentTimeMillis();

    try {
      Assertions.assertEquals(TOT, database.countType(TYPE_NAME, false));

      System.out.println("Lookup all the keys...");

      begin = System.currentTimeMillis();

      for (long id = 0; id < TOT; ++id) {
        final Cursor<RID> records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);
        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);

        final Document record = (Document) records.next().getRecord();
        Assertions.assertEquals("" + id, record.get("id"));

        if (id % 10000 == 0) {
          long delta = System.currentTimeMillis() - begin;
          if (delta < 1)
            delta = 1;
          LogManager.instance().info(this, "Checked " + id + " lookups in " + delta + "ms = " + (10000 / delta) + " lookups/msec");
          begin = System.currentTimeMillis();
        }
      }
    } finally {
      System.out.println("Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
      reopenDatabase();
    }
  }
}