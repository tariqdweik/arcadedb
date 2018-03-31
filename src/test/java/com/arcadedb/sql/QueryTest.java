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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTest {
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
  public void testScan() {

    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      OResultSet rs = db.query("SELECT FROM V", new HashMap<>());

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        OResult record = rs.next();
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<String>();
        for (String p : record.getPropertyNames())
          prop.add(p);

        record.getElement().toString();

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertTrue(prop.contains("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(TOT, total.get());

      db.commit();

    } finally {
      db.close();
    }

  }

  @Test
  public void testEqualsFiltering() {

    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      Map<String, Object> params = new HashMap<>();
      params.put(":surname", "Miner123");
      OResultSet rs = db.query("SELECT FROM V WHERE surname = :surname", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        OResult record = rs.next();
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<String>();
        for (String p : record.getPropertyNames())
          prop.add(p);

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertTrue(prop.contains("id"));
        Assertions.assertTrue(prop.contains("name"));
        Assertions.assertEquals("Miner123", record.getProperty("surname"));

        total.incrementAndGet();

      }

      Assertions.assertEquals(1, total.get());

      db.commit();

    } finally {
      db.close();
    }

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