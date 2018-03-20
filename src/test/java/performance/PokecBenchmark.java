package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PRecord;
import com.arcadedb.database.PRecordCallback;
import com.arcadedb.engine.PFile;
import com.arcadedb.utility.PLogManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Imports the POKEC relationships (https://snap.stanford.edu/data/soc-pokec.html)
 */
public class PokecBenchmark {

  private static final String DB_PATH = "target/database/pokec";

  public static void main(String[] args) throws Exception {
    new PokecBenchmark();
  }

  private PokecBenchmark() throws Exception {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();

    try {
      for (int i = 0; i < 20; ++i) {
        final long begin = System.currentTimeMillis();

        final Map<String, AtomicInteger> aggregate = new HashMap<>();
        db.scanType("V", new PRecordCallback() {
          @Override
          public boolean onRecord(final PRecord record) {
            String age = (String) record.get("age");

            AtomicInteger counter = aggregate.get(age);
            if (counter == null) {
              counter = new AtomicInteger(1);
              aggregate.put(age, counter);
            } else
              counter.incrementAndGet();

            return false;
          }
        });

        PLogManager.instance().info(this, "Elapsed: " + (System.currentTimeMillis() - begin));
      }

    } finally {
      db.close();
    }
  }
}