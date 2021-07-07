/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package performance;

import com.arcadedb.BaseTest;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceSQLSelect extends BaseTest {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10;

  public static void main(String[] args) {
    new PerformanceSQLSelect().run();
  }

  @Override
  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  private void run() {
    database.async().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        final ResultSet rs = database.command("SQL", "select from " + TYPE_NAME + " where id < 1l");
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((long) record.getProperty("id") < 1);
          row.incrementAndGet();
        }

        System.out
            .println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database.countType(TYPE_NAME, true) + ")");
      }
    } finally {
      database.close();
    }
  }
}