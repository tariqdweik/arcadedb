/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;

public class PerformanceScanParsing {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10000000;

  public static void main(String[] args) throws Exception {
    new PerformanceScanParsing().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).open();

    if (!database.getSchema().existsType(TYPE_NAME)) {
      database.getSchema().createVertexType(TYPE_NAME);
      database.begin();
      final ModifiableVertex v = database.newVertex(TYPE_NAME);
      v.set("name", "test");
      database.commit();
    }

    database.asynch().setParallelLevel(4);

    try {
      final long begin = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOPS; ++i) {

        final ResultSet rs = database.query("select from " + TYPE_NAME + " limit 1", null);
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
        }

      }

      System.out.println("Executed " + MAX_LOOPS + " simple queries in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
    }
  }
}