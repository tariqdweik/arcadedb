/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;

import java.util.UUID;

public class PerformanceIndexTest {
  private static final int    TOT       = 300000000;
  private static final String TYPE_NAME = "Device";

  public static void main(String[] args) throws Exception {
    new PerformanceIndexTest().run();
  }

  private void run() {
    GlobalConfiguration.PROFILE.setValue("high-performance");
    PerformanceTest.clean();

    final int parallel = 2;

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        DocumentType v = database.getSchema().createDocumentType(TYPE_NAME, parallel);

        v.createProperty("id", Long.class);
        v.createProperty("name", String.class);
        v.createProperty("surname", String.class);
        v.createProperty("locali", Integer.class);
        v.createProperty("notes1", String.class);

        database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "id" }, 5000000);

        database.commit();
      }
    } finally {
      database.close();
    }

    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.async().setCommitEvery(5000);
      database.async().setParallelLevel(parallel);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);

      database.async().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          System.out.println("ERROR: " + exception);
          exception.printStackTrace();
        }
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final MutableDocument record = database.newDocument(TYPE_NAME);

        final String randomString = UUID.randomUUID().toString();

        record.set("id", row);
        record.set("name", randomString);
        record.set("surname", randomString);
        record.set("locali", 10);
        record.set("notes1",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");
//        record.set("notes2",
//            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");

        database.async().createRecord(record);

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

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