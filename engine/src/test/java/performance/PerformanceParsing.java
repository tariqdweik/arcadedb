/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.async.SQLCallback;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicLong;

public class PerformanceParsing {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10000000;

  public static void main(String[] args) throws Exception {
    new PerformanceParsing().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.getSchema().createVertexType(TYPE_NAME);
      database.begin();
      final ModifiableVertex v = database.newVertex(TYPE_NAME);
      v.set("name", "test");
      database.commit();
    }

    database.asynch().setParallelLevel(4);

    final AtomicLong ok = new AtomicLong();
    final AtomicLong error = new AtomicLong();

    try {
      final long begin = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOPS; ++i) {

        database.asynch().command("SQL", "select from " + TYPE_NAME + " limit 1", null, new SQLCallback() {
          @Override
          public void onOk(final ResultSet rs) {
            ok.incrementAndGet();

            while (rs.hasNext()) {
              Result record = rs.next();
              Assertions.assertNotNull(record);
            }
          }

          @Override
          public void onError(Exception exception) {
            error.incrementAndGet();
          }
        });
      }

      System.out.println("Executed " + MAX_LOOPS + " simple queries in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();

      Assertions.assertEquals(MAX_LOOPS, ok.get());
      Assertions.assertEquals(0, error.get());
    }
  }
}