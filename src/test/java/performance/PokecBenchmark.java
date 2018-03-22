package performance;

import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;
import com.arcadedb.graph.PImmutableEdge3;
import com.arcadedb.graph.PVertex;
import com.arcadedb.utility.PLogManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
      aggregate(db);
      traverse(db);

    } finally {
      db.close();
    }
  }

  private void traverse(PDatabase db) {
    PLogManager.instance().info(this, "Traversing the entire graph...");

    for (int i = 0; i < 3; ++i) {
      final long begin = System.currentTimeMillis();

      final AtomicInteger rootTraversed = new AtomicInteger();
      final AtomicLong totalTraversed = new AtomicLong();

      db.scanType("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          final PVertex v = (PVertex) record;

          rootTraversed.incrementAndGet();

          for (final Iterator<PImmutableEdge3> neighbors = v.getConnectedVertices(PVertex.DIRECTION.OUT); neighbors.hasNext(); ) {
            final PImmutableEdge3 neighborEntry = neighbors.next();

            totalTraversed.incrementAndGet();

            final PVertex neighbor = (PVertex) neighborEntry.getTargetVertex().getRecord();

            for (final Iterator<PImmutableEdge3> neighbors2 = neighbor.getConnectedVertices(PVertex.DIRECTION.OUT); neighbors2
                .hasNext(); ) {
              final PImmutableEdge3 neighborEntry2 = neighbors2.next();
              final PRID neighbor2 = neighborEntry2.getTargetVertex().getIdentity();

              totalTraversed.incrementAndGet();
            }
          }

          if (rootTraversed.get() % 10 == 0) {
            PLogManager.instance().info(this, "- traversed %d roots - %d total", rootTraversed.get(), totalTraversed.get());
            PLogManager.instance().info(this, "- edges stats %s", db.getSchema().getIndexByName("edges").getStats());
          }

          return true;
        }
      });

      PLogManager.instance()
          .info(this, "- elapsed: " + (System.currentTimeMillis() - begin) + "traversed %d roots - %d total", rootTraversed.get(),
              totalTraversed.get());
    }
  }

  private void aggregate(PDatabase db) {
    PLogManager.instance().info(this, "Aggregating by age...");

    for (int i = 0; i < 3; ++i) {
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

          return true;
        }
      });

      PLogManager.instance().info(this, "- elapsed: " + (System.currentTimeMillis() - begin));
    }
  }
}