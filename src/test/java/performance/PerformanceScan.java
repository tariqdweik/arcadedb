package performance;

import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceScan {
  private static final String CLASS_NAME = "Person";
  private static final int    MAX_LOOPS  = 1;

  public static void main(String[] args) throws Exception {
    new PerformanceScan().run();
  }

  private void run() throws IOException {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_ONLY).useParallel(true)
        .acquire();

    if (database instanceof PDatabaseParallel) {
      ((PDatabaseParallel) database).setParallelLevel(4);
    }

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        database.scanType(CLASS_NAME, new PRecordCallback() {
          @Override
          public boolean onRecord(final PRecord record) {
            final PImmutableDocument document = ((PImmutableDocument) record);

            document.get("id");
//            for (String f : document.getPropertyNames()) {
//              Object o = document.get(f);
//            }

            if (row.incrementAndGet() % 1000000 == 0)
              System.out.println("- Scanned " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms");

            return true;
          }
        });

        System.out.println("Found " + row.get() + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }
    } finally {
      database.close();
    }
  }
}