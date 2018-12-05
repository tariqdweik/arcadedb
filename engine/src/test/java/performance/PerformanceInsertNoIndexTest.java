/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.BaseTest;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;

import java.util.logging.Level;

public class PerformanceInsertNoIndexTest extends BaseTest {
  private static final int    TOT       = 20000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 3;

  public static void main(String[] args) {
    new PerformanceInsertNoIndexTest().run();
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2,
          Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11,
          Object arg12) {
      }

      @Override
      public void flush() {
      }
    });

    return "high-performance";
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
        database.async().setParallelLevel(PARALLEL);
        database.async().setTransactionUseWAL(false);
        database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
        database.async().setCommitEvery(5000);
        database.async().onError(new ErrorCallback() {
          @Override
          public void call(Exception exception) {
            LogManager.instance().log(this, Level.SEVERE, "ERROR: " + exception, exception);
            System.exit(1);
          }
        });

        final long startTimer = System.currentTimeMillis();
        long lastLap = startTimer;
        long lastLapCounter = 0;

        long counter = 0;
        for (; counter < TOT; ++counter) {
          final MutableDocument record = database.newDocument(TYPE_NAME);

          record.set("id", counter);
          record.set("name", "Luca" + counter);
          record.set("surname", "Skywalker" + counter);
          record.set("locali", 10);

          database.async().createRecord(record,null);

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