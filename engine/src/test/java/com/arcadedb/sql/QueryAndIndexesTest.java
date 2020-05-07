/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryAndIndexesTest extends BaseTest {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V")) {
          VertexType t = database.getSchema().createVertexType("V");
          t.createProperty("name", String.class);
          t.createProperty("surname", String.class);
          database.getSchema().createTypeIndex(SchemaImpl.INDEX_TYPE.LSM_TREE, true, "V", new String[] { "name", "surname" });
        }

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
  public void testPartialMatchingFiltering() {

    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":name", "Jay");
        params.put(":surname", "Miner123");
        ResultSet rs = db.command("SQL", "SELECT FROM V WHERE name = :name", params);

        final AtomicInteger total = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          Assertions.assertNotNull(record);

          Set<String> prop = new HashSet<>();
          for (String p : record.getPropertyNames())
            prop.add(p);

          Assertions.assertEquals("Jay", record.getProperty("name"));

          total.incrementAndGet();
        }

        Assertions.assertEquals(TOT, total.get());
      }
    });
  }
}