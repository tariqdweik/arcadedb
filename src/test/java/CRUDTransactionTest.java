import com.arcadedb.database.*;
import com.arcadedb.exception.PDatabaseIsReadOnlyException;
import com.arcadedb.engine.PFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class CRUDTransactionTest {
  private static final int TOT = 10000;

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
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    final PDatabase db = new PDatabaseFactory("/temp/proton/testdb", PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      db.scanBucket("V", new PRecordCallback() {
        @Override
        public boolean onRecord(final PRecord record) {
          Assert.assertNotNull(record);

          Set<String> prop = new HashSet<String>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assert.assertEquals(3, record.getPropertyNames().size(),9);
          Assert.assertTrue(prop.contains("id"));
          Assert.assertTrue(prop.contains("name"));
          Assert.assertTrue(prop.contains("surname"));

          total.incrementAndGet();
          return true;
        }
      });

      Assert.assertEquals(TOT, total.get());

      db.commit();

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

        for (int i = 0; i < total; ++i) {
          final PModifiableDocument v = database.newDocument();
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save("V");
        }
      }
    });
  }
}