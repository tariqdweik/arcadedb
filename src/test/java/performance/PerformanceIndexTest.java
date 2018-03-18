package performance;

import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;
import com.arcadedb.schema.PDocumentType;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;

public class PerformanceIndexTest {
  private static final int    TOT       = 1000000;
  private static final String TYPE_NAME = "Person";

  public static void main(String[] args) throws Exception {
    new PerformanceIndexTest().run();
  }

  private void run() throws IOException {
    PerformanceTest.clean();

    final int parallel = 3;

    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final PDocumentType type = database.getSchema().createDocumentType(TYPE_NAME, parallel);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);

        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" }, 50000000);
        database.commit();
      }
    } finally {
      database.close();
    }

    database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_WRITE).useParallel(true).acquire();

    long begin = System.currentTimeMillis();

    try {

      if (database instanceof PDatabaseParallel) {
        ((PDatabaseParallel) database).setCommitEvery(5000);
        ((PDatabaseParallel) database).setParallelLevel(parallel);
      }

      database.begin();

      long row = 0;
      for (; row < TOT; ++row) {
        final PModifiableDocument record = database.newDocument(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        record.save();

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }
      database.commit();

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

    begin = System.currentTimeMillis();
    database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_ONLY).acquire();
    try {
      System.out.println("Lookup all the keys...");
      for (long id = 0; id < TOT; ++id) {
        final List<PBaseDocument> records = (List<PBaseDocument>) database
            .lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);
        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);

        final PBaseDocument record = records.get(0);
        Assertions.assertEquals(id, record.get("id"));

        if (id % 100000 == 0)
          System.out.println("Checked " + id + " lookups in " + (System.currentTimeMillis() - begin) + "ms");
      }
    } finally {
      database.close();
      System.out.println("Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }
}