/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class RemoteGraphTest extends BaseGraphRemoteTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void populate() {
    super.populate();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "target/databases");
    server = new ArcadeDBServer(config);
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
      Assertions.fail("Cannot start the server");
    }
  }

  @AfterEach
  public void drop() {
    server.stop();

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

      ResultSet resultset = database.command("select count(*) as count from V1");
      Result item = resultset.next();
      Assertions.assertEquals(1001, (int) item.getProperty("count"));

      database.command("update V1 set value = 1");

      resultset = database.command("select count(*) as count from V1 where value > 0 limit 1000");
      Assertions.assertTrue(resultset.hasNext());

      item = resultset.next();
      Assertions.assertEquals(1000, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }
}