/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTest extends BaseTest {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createVertexType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newVertex("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner" + i);

          v.save();
        }
      }
    });
  }

  @Test
  public void testScan() {

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        ResultSet rs = db.command("SQL", "SELECT FROM V", new HashMap<>());

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

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":name", "Jay");
        params.put(":surname", "Miner123");
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

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

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":name", "Jay");
        params.put(":surname", "Miner123");
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

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
        Assertions.assertTrue(((DatabaseInternal) db).getStatementCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

        // CHECK EXECUTION PLAN CACHE
        Assertions.assertTrue(((DatabaseInternal) db).getExecutionPlanCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

        // EXECUTE THE 2ND TIME
        rs = db.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", TOT - 11);
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE id > :id", params);

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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", TOT - 11);
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE id >= :id", params);

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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE id < :id", params);

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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE id <= :id", params);

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
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE NOT( id > :id )", params);

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
  public void testCreateVertexType() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        db.command("SQL", "CREATE VERTEX TYPE Foo");
        db.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
        db.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");

        ResultSet rs = db.query("SQL", "SELECT FROM Foo");
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }
}