/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.Index;

import java.io.IOException;

public class PerformanceIndexCompaction {
  public static void main(String[] args) throws Exception {
    new PerformanceIndexCompaction().run();
  }

  private void run() throws IOException {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).acquire();

    final long begin = System.currentTimeMillis();
    try {
      System.out.println("Compacting all indexes...");
      for (Index index : database.getSchema().getIndexes())
        index.compact();

      System.out.println("Compaction done");

    } finally {
      database.close();
      System.out.println("Compaction finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }
}