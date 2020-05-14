/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.logging.Level;

public class PerformanceIndexCompaction {
  public static void main(String[] args) throws Exception {
    new PerformanceIndexCompaction().run();
  }

  private void run() throws IOException, InterruptedException {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(5);

    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open(PaginatedFile.MODE.READ_WRITE);

    final long begin = System.currentTimeMillis();
    try {
      System.out.println("Compacting all indexes...");

      final long total = database.countType("Device", true);
      long totalIndexed = countIndexedItems(database);
      LogManager.instance().log(this, Level.INFO, "Total indexes items %d", null, totalIndexed);

      for (Index index : database.getSchema().getIndexes())
        Assertions.assertTrue(((IndexInternal) index).compact());

      long totalIndexed2 = countIndexedItems(database);

      Assertions.assertEquals(total, totalIndexed);
      Assertions.assertEquals(totalIndexed, totalIndexed2);

      System.out.println("Compaction done");

    } finally {
      database.close();
      System.out.println("Compaction finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }

  private long countIndexedItems(Database database) throws IOException {
    long totalIndexed = 0;
    for (Index index : database.getSchema().getIndexes()) {
      IndexCursor it = ((RangeIndex) index).iterator(true);
      while (it.hasNext()) {
        it.next();
        ++totalIndexed;
      }
    }
    return totalIndexed;
  }
}