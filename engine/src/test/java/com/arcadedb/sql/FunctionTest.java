package com.arcadedb.sql;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FunctionTest {
  private static final int    TOT     = 10000;
  private static final String DB_PATH = "target/database/testdb";

  @BeforeAll
  public static void populate() {
    populate(TOT);
  }

  @AfterAll
  public static void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testCountFunction() {
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        OResultSet rs = db.query("SELECT count(*) as count FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          OResult record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertFalse(record.getIdentity().isPresent());
          Assertions.assertEquals(10, ((Number) record.getProperty("count")).intValue());
          counter.incrementAndGet();
        }
        Assertions.assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testAvgFunction() {
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        OResultSet rs = db.query("SELECT avg(id) as avg FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          OResult record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertFalse(record.getIdentity().isPresent());
          Assertions.assertEquals(4, ((Number) record.getProperty("avg")).intValue());
          counter.incrementAndGet();
        }
        Assertions.assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testMaxFunction() {
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase db) {
        Map<String, Object> params = new HashMap<>();
        OResultSet rs = db.query("SELECT max(id) as max FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          OResult record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertFalse(record.getIdentity().isPresent());
          Assertions.assertEquals(TOT - 1, ((Number) record.getProperty("max")).intValue());
          counter.incrementAndGet();
        }
        Assertions.assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testMinFunction() {
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase db) {
        Map<String, Object> params = new HashMap<>();
        OResultSet rs = db.query("SELECT min(id) as min FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          OResult record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertFalse(record.getIdentity().isPresent());
          Assertions.assertEquals(0, ((Number) record.getProperty("min")).intValue());
          counter.incrementAndGet();
        }
        Assertions.assertEquals(1, counter.get());
      }
    });
  }

  private static void populate(final int total) {
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createVertexType("V");

        for (int i = 0; i < total; ++i) {
          final PModifiableDocument v = database.newVertex("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner" + i);

          v.save();
        }
      }
    });
  }
}