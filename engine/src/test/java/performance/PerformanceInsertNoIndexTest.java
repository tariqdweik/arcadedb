/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.LogManager;

public class PerformanceInsertNoIndexTest {
  private static final int    TOT       = 20000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = Runtime.getRuntime().availableProcessors() - 1;

  public static void main(String[] args) {
    new PerformanceInsertNoIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).open();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);

        database.commit();
      }
    } finally {
      database.close();
    }

    database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).open();

    long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().setTransactionUseWAL(false);
      database.asynch().setTransactionSync(WALFile.FLUSH_TYPE.NO);
      database.asynch().setCommitEvery(5000);
      database.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          LogManager.instance().error(this, "ERROR: " + exception, exception);
          System.exit(1);
        }
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final ModifiableDocument record = database.newDocument(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        database.asynch().createRecord(record);

        if (row % 1000000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }
}