/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.LogManager;

import java.util.UUID;

public class PerformanceVertexIndexTest {
  private static final int    TOT       = 5000000;
  private static final String TYPE_NAME = "Device";

  public static void main(String[] args) throws Exception {
    new PerformanceVertexIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    final int parallel = 4;

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        DocumentType v = database.getSchema().createDocumentType(TYPE_NAME, parallel);

        v.createProperty("id", String.class);
        v.createProperty("number", String.class);
        v.createProperty("relativeName", String.class);

        v.createProperty("lastModifiedUserId", String.class);
        v.createProperty("createdDate", String.class);
        v.createProperty("assocJointClosureId", String.class);
        v.createProperty("HolderSpec_Name", String.class);
        v.createProperty("Name", String.class);
        v.createProperty("holderGroupName", String.class);
        v.createProperty("slot2slottype", String.class);
        v.createProperty("inventoryStatus", String.class);
        v.createProperty("lastModifiedDate", String.class);
        v.createProperty("createdUserId", String.class);
        v.createProperty("orientation", String.class);
        v.createProperty("operationalStatus", String.class);
        v.createProperty("supplierName", String.class);

        database.getSchema().createClassIndexes(false, "Device", new String[] { "id" }, 2 * 1024 * 1024);
        database.getSchema().createClassIndexes(false, "Device", new String[] { "number" }, 2 * 1024 * 1024);
        database.getSchema().createClassIndexes(false, "Device", new String[] { "relativeName" }, 2 * 1024 * 1024);

        database.commit();
      }
    } finally {
      database.close();
    }

    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.asynch().setCommitEvery(10000);
      database.asynch().setParallelLevel(parallel);
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
        final ModifiableDocument v = database.newDocument("Device");

        final String randomString = UUID.randomUUID().toString();

        v.set("id", randomString); // INDEXED
        v.set("number", "" + counter); // INDEXED
        v.set("relativeName", "/shelf=" + counter + "/slot=1"); // INDEXED

        v.set("lastModifiedUserId", "Holder");
        v.set("createdDate", "2011-09-12 14:50:57.0");
        v.set("assocJointClosureId", "434746");
        v.set("HolderSpec_Name", "Slot");
        v.set("Name", "1");
        v.set("holderGroupName", "TBC");
        v.set("slot2slottype", "1900000012");
        v.set("inventoryStatus", "INI");
        v.set("lastModifiedDate", "2011-09-12 14:54:13.0");
        v.set("createdUserId", "Holder");
        v.set("orientation", "NA");
        v.set("operationalStatus", "NotAvailable");
        v.set("supplierName", "TBD");

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
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
//
//    begin = System.currentTimeMillis();
//    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
//    try {
//      System.out.println("Lookup all the keys...");
//      for (long id = 0; id < TOT; ++id) {
//        final Cursor<RID> records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
//        Assertions.assertNotNull(records);
//        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);
//
//        final Document record = (Document) records.next().getRecord();
//        Assertions.assertEquals(id, record.get("id"));
//
//        if (id % 100000 == 0)
//          System.out.println("Checked " + id + " lookups in " + (System.currentTimeMillis() - begin) + "ms");
//      }
//    } finally {
//      database.close();
//      System.out.println("Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
//    }

  }
}