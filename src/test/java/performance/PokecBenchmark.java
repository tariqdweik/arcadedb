package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PRecord;
import com.arcadedb.database.PRecordCallback;
import com.arcadedb.engine.PFile;

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

    final Map<Integer, AtomicInteger> aggregate = new HashMap<>();

    try {
      db.scanType("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          Integer age = (Integer) record.get("age");

          AtomicInteger counter = aggregate.get(age);
          if (counter == null) {
            counter = new AtomicInteger(1);
            aggregate.put(age, counter);
          } else
            counter.incrementAndGet();

          return false;
        }
      });



    } finally {
      db.close();
    }
  }
}