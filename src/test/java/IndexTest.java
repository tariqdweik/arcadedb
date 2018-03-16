import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.engine.PFile;
import com.arcadedb.engine.PIndex;
import com.arcadedb.engine.PIndexIterator;
import com.arcadedb.schema.PType;
import com.arcadedb.utility.PFileUtils;
import junit.framework.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class IndexTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final String DB_PATH   = "target/database/testdb";

  @BeforeAll
  public static void populate() {
    populate(TOT);
  }

  @AfterAll
  public static void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testPopulate() {
  }

  @Test
  public void testScanIndexAscending() throws IOException {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexIterator iterator = index.newIterator(true);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

          total++;
        }
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexDescending() throws IOException {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexIterator iterator = index.newIterator(false);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

          total++;
        }
      }

      Assertions.assertEquals(TOT, total);

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanIndexAscendingPartial() throws IOException {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      int total = 0;

      final PIndex[] indexes = db.getSchema().getIndexes();
      for (PIndex index : indexes) {
        Assertions.assertNotNull(index);

        final PIndexIterator iterator = index.newIterator(true, 0, 9);
        Assertions.assertNotNull(iterator);

        while (iterator.hasNext()) {
          iterator.next();

          Assertions.assertNotNull(iterator.getKeys());
          Assertions.assertEquals(1, iterator.getKeys().length);

          Assertions.assertNotNull(iterator.getValue());

          total++;
        }
      }

      Assertions.assertEquals(TOT - 30, total);

    } finally {
      db.close();
    }
  }

  private static void populate(final int total) {
    PFileUtils.deleteRecursively(new File(DB_PATH));

    new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        Assert.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final PType type = database.getSchema().createType(TYPE_NAME, 3);
        type.createProperty("id", Integer.class);
        database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" }, 20000);

        for (int i = 0; i < total; ++i) {
          final PModifiableDocument v = database.newDocument();
          v.setType(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");

          v.save();
        }
      }
    });
  }
}