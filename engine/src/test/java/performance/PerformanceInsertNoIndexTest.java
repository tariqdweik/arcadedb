package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.async.PErrorCallback;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PLogManager;

public class PerformanceInsertNoIndexTest {
  private static final int    TOT       = 100000000;
  private static final String TYPE_NAME = "Person";
  private static final int    PARALLEL  = 2;

  public static void main(String[] args) {
    new PerformanceInsertNoIndexTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
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

    database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    try {

      database.setReadYourWrites(false);
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().setTransactionUseWAL(false);
      database.asynch().setTransactionSync(false);
      database.asynch().setCommitEvery(20000);
      database.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {
          PLogManager.instance().error(this, "ERROR: " + exception, exception);
          System.exit(1);
        }
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final PModifiableDocument record = database.newDocument(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        database.asynch().createRecord(record);

        if (row % 1000000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }
}