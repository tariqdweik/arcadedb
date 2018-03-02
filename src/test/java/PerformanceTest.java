import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;
import com.arcadedb.exception.PDatabaseIsReadOnlyException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest {
  private static final int TOT = 10000000;

  @Before
  public void populate() {
    populate(TOT);
  }

  @After
  public void drop() {
    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScanWithFiltering() {

    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_ONLY).acquire();

    try {
      for (int i = 0; i < 10; ++i) {
        final AtomicInteger total = new AtomicInteger();
        final AtomicInteger found = new AtomicInteger();
        db.scanBucket("V", new PRecordCallback() {
          @Override
          public boolean onRecord(final PRecord record) {
            if ((Integer) record.get("id") < 10)
              found.incrementAndGet();

            total.incrementAndGet();
            return true;
          }
        });

        Assert.assertEquals(10, found.get());
        Assert.assertEquals(TOT, total.get());
      }

    } finally {
      db.close();
    }
  }

  @Test
  public void testLookupAllRecords() {
    final AtomicInteger total = new AtomicInteger();

    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanBucket("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          final PRecord record2 = db.lookupByRID(record.getIdentity());
          Assert.assertNotNull(record2);
          Assert.assertEquals(record, record2);

          Set<String> prop = new HashSet<String>();
          for (String p : record2.getPropertyNames())
            prop.add(p);

          Assert.assertEquals(record2.getPropertyNames().size(), 3);
          Assert.assertTrue(prop.contains("id"));
          Assert.assertTrue(prop.contains("name"));
          Assert.assertTrue(prop.contains("surname"));

          total.incrementAndGet();
          return true;
        }
      });

      db.commit();

    } finally {
      db.close();
    }

    Assert.assertEquals(TOT, total.get());
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      db.scanBucket("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          db.deleteRecord(record.getIdentity());
          total.incrementAndGet();
          return true;
        }
      });

      db.commit();

    } finally {
      db.close();
    }

    Assert.assertEquals(TOT, total.get());

    populate();

    final PDatabase db2 = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_WRITE).acquire();
    db2.begin();
    try {
      Assert.assertEquals(TOT, db2.countBucket("V"));
      db2.commit();
    } finally {
      db2.close();
    }
  }

  @Test(expected = PDatabaseIsReadOnlyException.class)
  public void testDeleteFail() {
    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanBucket("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          db.deleteRecord(record.getIdentity());
          return true;
        }
      });

      db.commit();

    } finally {
      db.close();
    }
  }

  private void populate(final int total) {
    new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        if (!database.getSchema().existsBucket("V"))
          database.getSchema().createBucket("V");

        final long begin = System.currentTimeMillis();

        for (int i = 0; i < total; ++i) {
          final PModifiableDocument v = database.newDocument();
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save("V");
        }

        System.out.println("Inserted " + total + " records in " + (System.currentTimeMillis() - begin));

        Assert.assertEquals(TOT, database.countBucket("V"));
      }
    });
  }
}
