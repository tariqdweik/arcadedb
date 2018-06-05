/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceScanSQL {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10;

  public static void main(String[] args) throws Exception {
    new PerformanceScanSQL().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).open();

    database.asynch().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        final ResultSet rs = database.query("select from " + TYPE_NAME + " where id < 1l", null);
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((long) record.getProperty("id") < 1);
          row.incrementAndGet();
        }

        System.out.println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database
            .countType(TYPE_NAME, true) + ")");
      }
    } finally {
      database.close();
    }
  }
}