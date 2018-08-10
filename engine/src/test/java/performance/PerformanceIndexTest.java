/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.*;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Assertions;

import java.util.UUID;

public class PerformanceIndexTest {
  private static final int    TOT       = 5000000;
  private static final String TYPE_NAME = "Person";

  public static void main(String[] args) throws Exception {
    new PerformanceIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    final int parallel = 2;

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, parallel);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);
        type.createProperty("notes1", String.class);
//        type.createProperty("notes2", String.class);

        database.getSchema().createClassIndexes(false, TYPE_NAME, new String[] { "id" }, 3000000);
        database.getSchema().createClassIndexes(false, TYPE_NAME, new String[] { "name" }, 3000000);
        database.getSchema().createClassIndexes(false, TYPE_NAME, new String[] { "surname" }, 3000000);
        database.commit();
      }
    } finally {
      database.close();
    }

    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.asynch().setCommitEvery(5000);
      database.asynch().setParallelLevel(parallel);
      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(WALFile.FLUSH_TYPE.YES_NO_METADATA);

      database.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          System.out.println("ERROR: " + exception);
          exception.printStackTrace();
        }
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final ModifiableDocument record = database.newDocument(TYPE_NAME);

        final String randomString = UUID.randomUUID().toString();

        record.set("id", row);
        record.set("name", randomString);
        record.set("surname", randomString);
        record.set("locali", 10);
        record.set("notes1",
            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");
//        record.set("notes2",
//            "This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields. This is a long field to check how Arcade behaves with large fields.");

        database.asynch().createRecord(record);

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

    begin = System.currentTimeMillis();
    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();
    try {
      System.out.println("Lookup all the keys...");
      for (long id = 0; id < TOT; ++id) {
        final Cursor<RID> records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);
        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);

        final Document record = (Document) records.next().getRecord();
        Assertions.assertEquals(id, record.get("id"));

        if (id % 100000 == 0)
          System.out.println("Checked " + id + " lookups in " + (System.currentTimeMillis() - begin) + "ms");
      }
    } finally {
      database.close();
      System.out.println("Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }
}