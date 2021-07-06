/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.sql.executor.ResultSet;

import java.util.concurrent.atomic.AtomicLong;

public class PerformanceSQLInsert {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10_000_000;

  public static void main(String[] args) {
    new PerformanceSQLInsert().run();
  }

  private void run() {
    final DatabaseFactory factory = new DatabaseFactory(PerformanceTest.DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    final Database database = factory.create();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.getSchema().createVertexType(TYPE_NAME);
    }

    database.async().setCommitEvery(1);
//    database.asynch().setParallelLevel(2);

    final AtomicLong oks = new AtomicLong();
    final AtomicLong errors = new AtomicLong();

    try {
      final long begin = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOPS; ++i) {
        database.async().command("SQL", "insert into " + TYPE_NAME + " set id = " + i + ", name = 'Luca'", new AsyncResultsetCallback() {
          @Override
          public void onOk(ResultSet resultset) {
            oks.incrementAndGet();
          }

          @Override
          public void onError(Exception exception) {
            errors.incrementAndGet();
          }
        });

        if (i % 100000 == 0)
          System.out.println(
              "Inserting " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok="
                  + oks.get() + " errors=" + errors.get() + ")");
      }

      System.out.println(
          "Inserting " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok=" + oks
              .get() + " errors=" + errors.get() + ")");

      while (oks.get() < MAX_LOOPS) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }

        System.out.println(
            "Inserted " + MAX_LOOPS + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + " ok=" + oks
                .get() + " errors=" + errors.get() + ")");
      }

    } finally {
      database.close();
    }
  }
}