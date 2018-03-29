package performance;

import com.arcadedb.database.PCursor;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import com.arcadedb.schema.PDocumentType;
import org.junit.jupiter.api.Assertions;

public class PerformanceVertexIndexTest {
  private static final int    TOT       = 10000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 2;

  public static void main(String[] args) throws Exception {
    new PerformanceVertexIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    long begin = System.currentTimeMillis();

    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final PDocumentType type = database.getSchema().createVertexType(TYPE_NAME, PARALLEL);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);

        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" });
        database.commit();
      }

      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(true);
      database.asynch().setCommitEvery(5000);
      database.asynch().setParallelLevel(PARALLEL);

      long row = 0;
      for (; row < TOT; ++row) {
        final PModifiableVertex record = database.newVertex(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        database.asynch().createRecord(record);

        if (row % 100000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      database.asynch().waitCompletion();

      System.out.println("Lookup all the keys...");
      for (long id = 0; id < TOT; ++id) {
        final PCursor<PRID> records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);

        if (records.size() > 1) {
          for (PRID r : records)
            System.out.println("FOUND " + r.getRecord());
        }

        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);

        final PVertex record = (PVertex) records.next().getRecord();
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