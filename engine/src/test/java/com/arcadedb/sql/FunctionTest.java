/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FunctionTest extends BaseTest {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(new Database.Transaction() {
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
  public void testCountFunction() {
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT count(*) as count FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
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
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT avg(id) as avg FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
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
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        ResultSet rs = db.command("SQL", "SELECT max(id) as max FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
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
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        ResultSet rs = db.command("SQL", "SELECT min(id) as min FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);
          Assertions.assertFalse(record.getIdentity().isPresent());
          Assertions.assertEquals(0, ((Number) record.getProperty("min")).intValue());
          counter.incrementAndGet();
        }
        Assertions.assertEquals(1, counter.get());
      }
    });
  }
}