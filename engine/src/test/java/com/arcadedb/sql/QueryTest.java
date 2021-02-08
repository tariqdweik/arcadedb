/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
        Assertions.assertTrue(
            ((DatabaseInternal) db).getStatementCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

        // CHECK EXECUTION PLAN CACHE
        Assertions.assertTrue(
            ((DatabaseInternal) db).getExecutionPlanCache().contains("SELECT FROM V WHERE name = :name AND surname = :surname"));

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

  @Test
  public void testCreateEdge() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        db.command("SQL", "CREATE VERTEX TYPE Foo");
        db.command("SQL", "CREATE EDGE TYPE TheEdge");
        db.command("SQL", "CREATE VERTEX Foo SET name = 'foo'");
        db.command("SQL", "CREATE VERTEX Foo SET name = 'bar'");
        db.command("SQL", "CREATE EDGE TheEdge FROM (SELECT FROM Foo WHERE name ='foo') TO (SELECT FROM Foo WHERE name ='bar')");

        ResultSet rs = db.query("SQL", "SELECT FROM TheEdge");
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }

  @Test
  public void testQueryEdge() {
    final String vertexClass = "testQueryEdge_V";
    final String edgeClass = "testQueryEdge_E";
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        db.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
        db.command("SQL", "CREATE EDGE TYPE " + edgeClass);
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
        db.command("SQL",
            "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
                + " WHERE name ='bar')");

        ResultSet rs = db.query("SQL", "SELECT FROM " + edgeClass);
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();

        rs = db.query("SQL", "SELECT out()[0].name as name from " + vertexClass + " where name = 'foo'");
        Assertions.assertTrue(rs.hasNext());
        Result item = rs.next();
        String name = item.getProperty("name");
        Assertions.assertTrue(name.contains("bar"));
        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }

  @Test
  public void testMethod() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        ResultSet rs = db.query("SQL", "SELECT 'bar'.prefix('foo') as name");
        Assertions.assertTrue(rs.hasNext());
        Assertions.assertEquals("foobar", rs.next().getProperty("name"));

        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }

  @Test
  public void testMatch() {
    final String vertexClass = "testMatch_V";
    final String edgeClass = "testMatch_E";
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        db.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
        db.command("SQL", "CREATE EDGE TYPE " + edgeClass);
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
        db.command("SQL",
            "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
                + " WHERE name ='bar')");

        ResultSet rs = db.query("SQL", "SELECT FROM " + edgeClass);
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();

        rs = db.query("SQL", "MATCH {type:" + vertexClass + ", as:a} -"+edgeClass+"->{}  RETURN $patterns");
        System.out.println(rs.getExecutionPlan().get().prettyPrint(0, 2));
        Assertions.assertTrue(rs.hasNext());
        Result item = rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }


  @Test
  public void testAnonMatch() {
    final String vertexClass = "testAnonMatch_V";
    final String edgeClass = "testAnonMatch_E";
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        db.command("SQL", "CREATE VERTEX TYPE " + vertexClass);
        db.command("SQL", "CREATE EDGE TYPE " + edgeClass);
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'foo'");
        db.command("SQL", "CREATE VERTEX " + vertexClass + " SET name = 'bar'");
        db.command("SQL",
            "CREATE EDGE " + edgeClass + " FROM (SELECT FROM " + vertexClass + " WHERE name ='foo') TO (SELECT FROM " + vertexClass
                + " WHERE name ='bar')");

        ResultSet rs = db.query("SQL", "SELECT FROM " + edgeClass);
        Assertions.assertTrue(rs.hasNext());
        rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();

        rs = db.query("SQL", "MATCH {type:" + vertexClass + ", as:a} --> {}  RETURN $patterns");
        System.out.println(rs.getExecutionPlan().get().prettyPrint(0, 2));
        Assertions.assertTrue(rs.hasNext());
        Result item = rs.next();
        Assertions.assertFalse(rs.hasNext());

        rs.close();
      }
    });
  }
}

