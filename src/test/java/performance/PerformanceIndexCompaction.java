package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PFile;
import com.arcadedb.index.PIndex;

import java.io.IOException;

public class PerformanceIndexCompaction {
  public static void main(String[] args) throws Exception {
    new PerformanceIndexCompaction().run();
  }

  private void run() throws IOException {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_WRITE).acquire();

    final long begin = System.currentTimeMillis();
    try {
      System.out.println("Compacting all indexes...");
      for (PIndex index : database.getSchema().getIndexes())
        index.compact();

      System.out.println("Compaction done");

    } finally {
      database.close();
      System.out.println("Compaction finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }
}