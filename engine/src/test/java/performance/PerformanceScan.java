/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package performance;

import com.arcadedb.database.*;

import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceScan {
  private static final String USERTYPE_NAME = "Person";
  private static final int    MAX_LOOPS  = 10;

  public static void main(String[] args) throws Exception {
    new PerformanceScan().run();
  }

  private void run() {
    final Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH).open();

    database.async().setParallelLevel(4);

    try {
      for (int i = 0; i < MAX_LOOPS; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        database.async().scanType(USERTYPE_NAME, true, new DocumentCallback() {
          @Override
          public boolean onRecord(final Document record) {
            final ImmutableDocument document = ((ImmutableDocument) record);

            document.get("id");

            if (row.incrementAndGet() % 10000000 == 0)
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