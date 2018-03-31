package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultSet;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceScanSQL {
  private static final String TYPE_NAME = "Person";
  private static final int    MAX_LOOPS = 10;

  public static void main(String[] args) throws Exception {
    new PerformanceScanSQL().run();
  }

  private void run() {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();

    database.asynch().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        final OResultSet rs = database.query("select from " + TYPE_NAME + " where id < 1l", null);
        while (rs.hasNext()) {
          OResult record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((long) record.getProperty("id") < 1);
          row.incrementAndGet();
        }

        System.out.println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms (Total=" + database
            .countType(TYPE_NAME) + ")");
      }
    } finally {
      database.close();
    }
  }
}