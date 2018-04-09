package performance;

import com.arcadedb.database.*;
import com.arcadedb.database.async.PErrorCallback;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PVertexType;
import com.arcadedb.utility.PLogManager;
import org.junit.jupiter.api.Assertions;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class RandomTest {
  private static final int TOT_ACCOUNT = 10000;
  private static final int TOT_TX      = 100000000;
  private static final int PARALLEL    = Runtime.getRuntime().availableProcessors();

  public static void main(String[] args) {
    new RandomTest().run();
  }

  private void run() {
    PerformanceTest.clean();

    createSchema();

    populateDatabase();

    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    database.asynch().setParallelLevel(PARALLEL);

    long begin = System.currentTimeMillis();

    try {
      final Random rnd = new Random();

      for (long txId = 0; txId < TOT_TX; ++txId) {
        database.asynch().transaction(new PDatabase.PTransaction() {
          @Override
          public void execute(PDatabase database) {
            final PModifiableDocument tx = database.newVertex("Transaction");
            tx.set("uuid", UUID.randomUUID().toString());
            tx.set("date", new Date());
            tx.set("amount", rnd.nextInt(1000000));
            tx.save();

            final PCursor<PRID> account = database
                .lookupByKey("Account", new String[] { "id" }, new Object[] { rnd.nextInt(TOT_ACCOUNT) });

            Assertions.assertTrue(account.hasNext());

            ((PModifiableVertex) tx).newEdge("Purchased", account.next(), true, "date", new Date());
          }
        });

        if (txId % 1000 == 0)
          System.out.println(
              "- Executed " + txId + " transactions in " + (System.currentTimeMillis() - begin) + "ms (threadId=" + Thread
                  .currentThread().getId() + ")");
      }

    } finally {
      database.close();
      System.out.println("Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void populateDatabase() {
    final PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    long begin = System.currentTimeMillis();

    try {
      database.asynch().setParallelLevel(PARALLEL);
      database.asynch().setTransactionUseWAL(true);
      database.asynch().setTransactionSync(true);
      database.asynch().setCommitEvery(20000);
      database.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {
          PLogManager.instance().error(this, "ERROR: " + exception, exception);
        }
      });

      for (long row = 0; row < TOT_ACCOUNT; ++row) {
        final PModifiableDocument record = database.newVertex("Account");
        record.set("id", row);
        record.set("name", "Luca" + row);
        record.set("surname", "Skywalker" + row);
        record.set("registered", new Date());
        database.asynch().createRecord(record);
      }

    } finally {
      database.close();
      System.out.println("Database populate finished in " + (System.currentTimeMillis() - begin) + "ms");
    }
  }

  private void createSchema() {
    PDatabase database = new PDatabaseFactory(PerformanceTest.DATABASE_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {
      if (!database.getSchema().existsType("Account")) {
        database.begin();

        final PVertexType accountType = database.getSchema().createVertexType("Account", PARALLEL);
        accountType.createProperty("id", Long.class);
        accountType.createProperty("name", String.class);
        accountType.createProperty("surname", String.class);
        accountType.createProperty("registered", Date.class);

        database.getSchema().createClassIndexes("Account", new String[] { "id" }, 5000000);

        final PVertexType txType = database.getSchema().createVertexType("Transaction", PARALLEL);
        txType.createProperty("uuid", String.class);
        txType.createProperty("date", Date.class);
        txType.createProperty("amount", BigDecimal.class);

        database.getSchema().createClassIndexes("Transaction", new String[] { "uuid" }, 5000000);

        final PEdgeType edgeType = database.getSchema().createEdgeType("Purchased", PARALLEL);
        edgeType.createProperty("date", Date.class);

        database.commit();
      }
    } finally {
      database.close();
    }
  }
}