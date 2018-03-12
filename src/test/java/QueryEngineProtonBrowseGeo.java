import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryEngineProtonBrowseGeo {
  public static void main(String[] args) throws Exception {
    new QueryEngineProtonBrowseGeo().run();
  }

  private void run() throws IOException {
    final PDatabase database = new PDatabaseFactory("/temp/proton/geodb", PFile.MODE.READ_ONLY).acquire();
    try {
      for (int i = 0; i < 2; ++i) {
        final long begin = System.currentTimeMillis();

        final AtomicInteger row = new AtomicInteger();

        database.scanBucket("Geo", new PRecordCallback() {
          @Override
          public boolean onRecord(final PRecord record) {
            final PModifiableDocument document = ((PImmutableDocument) record).modify();
            for (String f : document.getPropertyNames()) {
              Object o = document.get(f);
            }

            row.incrementAndGet();
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