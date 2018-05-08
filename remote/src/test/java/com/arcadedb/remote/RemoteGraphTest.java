/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.remote;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.server.HttpServer;
import com.arcadedb.server.HttpServerConfiguration;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoteGraphTest extends BaseGraphRemoteTest {
  private HttpServer server;

  @BeforeEach
  public void populate() {
    super.populate();

    final HttpServerConfiguration config = new HttpServerConfiguration();
    config.databaseDirectory = "target/database";
    server = new HttpServer(config);
    server.start();
  }

  @AfterEach
  public void drop() {
    server.close();

    final Database db = new DatabaseFactory(BaseGraphRemoteTest.DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void checkQuery() {
    final RemoteDatabase database = new RemoteDatabase("localhost", 2480, "graph");
    try {
      ResultSet resultset = database.command("select from V1 limit 1");

      Assertions.assertTrue(resultset.hasNext());

      final Result record = resultset.next();
      Assertions.assertEquals("V1", record.getProperty("name"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkInsert() {
    final RemoteDatabase database = new RemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 10000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      ResultSet resultset = database.command("select count(*) as count from V1");
      Assertions.assertTrue(resultset.hasNext());

      final Result item = resultset.next();
      Assertions.assertEquals(10001, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkDelete() {
    final RemoteDatabase database = new RemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 1000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      database.command("delete vertex V1 limit 101");

      ResultSet resultset = database.command("select count(*) as count from V1");
      Assertions.assertTrue(resultset.hasNext());

      final Result item = resultset.next();
      Assertions.assertEquals(900, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkUpdate() {
    final RemoteDatabase database = new RemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 1000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      database.command("update V1 set value = 1");

      ResultSet resultset = database.command("select count(*) as count from V1 where value > 0");
      Assertions.assertTrue(resultset.hasNext());

      final Result item = resultset.next();
      Assertions.assertEquals(1000, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }
}