package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.engine.PFile;
import com.arcadedb.schema.PDocumentType;

public class PerformanceInsertNoIndexTest {
  private static final int    TOT       = 100000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 2;

  public static void main(String[] args) throws Exception {
    new PerformanceInsertNoIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final PDocumentType type = database.getSchema().createDocumentType(TYPE_NAME, PARALLEL);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);

        database.commit();
      }
    } finally {
      database.close();
    }

    database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    try {

      database.asynch().setCommitEvery(30000);
      database.asynch().setParallelLevel(PARALLEL);

      long row = 0;
      for (; row < TOT; ++row) {
        final PModifiableDocument record = database.newDocument(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        database.asynch().createRecord(record);

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }
}