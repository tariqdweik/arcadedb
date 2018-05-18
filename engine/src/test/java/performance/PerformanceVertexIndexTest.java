/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.database.Cursor;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
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

    Database database = new DatabaseFactory(PerformanceTest.DATABASE_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        database.begin();

        final DocumentType type = database.getSchema().createVertexType(TYPE_NAME, PARALLEL);

        type.createProperty("id", Long.class);
        type.createProperty("name", String.class);
        type.createProperty("surname", String.class);
        type.createProperty("locali", Integer.class);

        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" });
        database.commit();
      }

      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(false);
      database.asynch().setCommitEvery(5000);
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          System.out.println("ERROR: " + exception);
          exception.printStackTrace();
        }
      });

      long row = 0;
      for (; row < TOT; ++row) {
        final ModifiableVertex record = database.newVertex(TYPE_NAME);

        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("locali", 10);

        database.asynch().createRecord(record);

        if (row % 1000000 == 0)
          System.out.println("Written " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");
      }

      System.out.println("Inserted " + row + " elements in " + (System.currentTimeMillis() - begin) + "ms");

      database.asynch().waitCompletion();

      System.out.println("Lookup all the keys...");
      for (long id = 0; id < TOT; ++id) {
        final Cursor<RID> records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
        Assertions.assertNotNull(records);

        if (records.size() > 1) {
          for (RID r : records)
            System.out.println("FOUND " + r.getRecord());
        }

        Assertions.assertEquals(1, records.size(), "Wrong result for lookup of key " + id);

        final Vertex record = (Vertex) records.next().getRecord();
        Assertions.assertEquals(id, record.get("id"));

        if (id % 1000000 == 0)
          System.out.println("Checked " + id + " lookups in " + (System.currentTimeMillis() - begin) + "ms");
      }
    } finally {
      database.close();
      System.out.println("Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

  }
}