/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.BaseTest;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.LogManager;

public class PerformanceInsertNoIndexTest extends BaseTest {
  private static final int    TOT       = 20000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 2;

  public static void main(String[] args) {
    new PerformanceInsertNoIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.begin();

      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

      type.createProperty("id", Long.class);
      type.createProperty("name", String.class);
      type.createProperty("surname", String.class);
      type.createProperty("locali", Integer.class);

      database.commit();

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

        final long startTimer = System.currentTimeMillis();
        long lastLap = startTimer;
        long lastLapCounter = 0;

        long counter = 0;
        for (; counter < TOT; ++counter) {
          final ModifiableDocument record = database.newDocument(TYPE_NAME);

          record.set("id", counter);
          record.set("name", "Luca" + counter);
          record.set("surname", "Skywalker" + counter);
          record.set("locali", 10);

          database.asynch().createRecord(record);

          if (counter % 1000000 == 0)
            System.out.println("Written " + counter + " elements in " + (System.currentTimeMillis() - begin) + "ms");
        }

        System.out.println("Inserted " + counter + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      } finally {
        System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
      }
    }

    database.close();
  }
}