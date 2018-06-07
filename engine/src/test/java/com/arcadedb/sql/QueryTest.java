/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
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
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.drop();
  }

  @Test
  public void testScan() {

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        ResultSet rs = db.query("SELECT FROM V", new HashMap<>());

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
          Assertions.assertTrue(prop.contains("id"));
          Assertions.assertTrue(prop.contains("name"));
          Assertions.assertTrue(prop.contains("surname"));

          total.incrementAndGet();
        }

        Assertions.assertEquals(TOT, total.get());
      }
    });
  }

  @Test
  public void testEqualsFiltering() {

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":name", "Jay");
        params.put(":surname", "Miner123");
        ResultSet rs = db.query("SELECT FROM V WHERE name = :name AND surname = :surname", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
          Assertions.assertEquals(123, (int) record.getProperty("id"));
          Assertions.assertEquals("Jay", record.getProperty("name"));
          Assertions.assertEquals("Miner123", record.getProperty("surname"));

          total.incrementAndGet();
        }

        Assertions.assertEquals(1, total.get());
      }
    });
  }

  @Test
  public void testCachedStatementAndExecutionPlan() {

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":name", "Jay");
        params.put(":surname", "Miner123");
        ResultSet rs = db.query("SELECT FROM V WHERE name = :name AND surname = :surname", params);

        AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
          Assertions.assertEquals(123, (int) record.getProperty("id"));
          Assertions.assertEquals("Jay", record.getProperty("name"));
          Assertions.assertEquals("Miner123", record.getProperty("surname"));

          total.incrementAndGet();
        }

        Assertions.assertEquals(1, total.get());

        // CHECK STATEMENT CACHE
        Assertions.assertTrue(
            ((DatabaseInternal) db).getStatementCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

        // CHECK EXECUTION PLAN CACHE
        Assertions.assertTrue(
            ((DatabaseInternal) db).getExecutionPlanCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

        // EXECUTE THE 2ND TIME
        rs = db.query("SELECT FROM V WHERE name = :name AND surname = :surname", params);

        total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
          Assertions.assertEquals(123, (int) record.getProperty("id"));
          Assertions.assertEquals("Jay", record.getProperty("name"));
          Assertions.assertEquals("Miner123", record.getProperty("surname"));

          total.incrementAndGet();
        }

        Assertions.assertEquals(1, total.get());
      }
    });
  }

  @Test
  public void testMajorFiltering() {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", TOT - 11);
        ResultSet rs = db.query("SELECT FROM V WHERE id > :id", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((int) record.getProperty("id") > TOT - 11);
          total.incrementAndGet();
        }
        Assertions.assertEquals(10, total.get());
      }
    });
  }

  @Test
  public void testMajorEqualsFiltering() {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", TOT - 11);
        ResultSet rs = db.query("SELECT FROM V WHERE id >= :id", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((int) record.getProperty("id") >= TOT - 11);
          total.incrementAndGet();
        }
        Assertions.assertEquals(11, total.get());
      }
    });
  }

  @Test
  public void testMinorFiltering() {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.query("SELECT FROM V WHERE id < :id", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((int) record.getProperty("id") < 10);
          total.incrementAndGet();
        }
        Assertions.assertEquals(10, total.get());
      }
    });
  }

  @Test
  public void testMinorEqualsFiltering() {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.query("SELECT FROM V WHERE id <= :id", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((int) record.getProperty("id") <= 10);
          total.incrementAndGet();
        }
        Assertions.assertEquals(11, total.get());
      }
    });
  }

  @Test
  public void testNotFiltering() {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.query("SELECT FROM V WHERE NOT( id > :id )", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertTrue((int) record.getProperty("id") <= 10);
          total.incrementAndGet();
        }
        Assertions.assertEquals(11, total.get());
      }
    });
  }

  private static void populate(final int total) {
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createVertexType("V");

        for (int i = 0; i < total; ++i) {
          final ModifiableDocument v = database.newVertex("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner" + i);

          v.save();
        }
      }
    });
  }
}