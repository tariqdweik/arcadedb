package com.arcadedb.remote;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.server.PHttpServer;
import com.arcadedb.server.PHttpServerConfiguration;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RemoteGraphTest extends BaseGraphRemoteTest {
  private PHttpServer server;

  @BeforeEach
  public void populate() {
    super.populate();

    final PHttpServerConfiguration config = new PHttpServerConfiguration();
    config.databaseDirectory = "target/database";
    server = new PHttpServer(config);
    server.start();
  }

  @AfterEach
  public void drop() {
    server.close();

    final PDatabase db = new PDatabaseFactory(BaseGraphRemoteTest.DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void checkQuery() {
    final PRemoteDatabase database = new PRemoteDatabase("localhost", 2480, "graph");
    try {
      OResultSet resultset = database.command("select from V1 limit 1");

      Assertions.assertTrue(resultset.hasNext());

      final OResult record = resultset.next();
      Assertions.assertEquals("V1", record.getProperty("name"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkInsert() {
    final PRemoteDatabase database = new PRemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 10000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      OResultSet resultset = database.command("select count(*) as count from V1");
      Assertions.assertTrue(resultset.hasNext());

      final OResult item = resultset.next();
      Assertions.assertEquals(10001, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkDelete() {
    final PRemoteDatabase database = new PRemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 1000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      database.command("delete vertex V1 limit 101");

      OResultSet resultset = database.command("select count(*) as count from V1");
      Assertions.assertTrue(resultset.hasNext());

      final OResult item = resultset.next();
      Assertions.assertEquals(900, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }

  @Test
  public void checkUpdate() {
    final PRemoteDatabase database = new PRemoteDatabase("localhost", 2480, "graph");
    try {

      for (int i = 0; i < 1000; ++i) {
        database.command("create vertex V1 set id = " + i + ", name = 'Jay', surname='Miner'");
      }

      database.command("update V1 set value = 1");

      OResultSet resultset = database.command("select count(*) as count from V1 where value > 0");
      Assertions.assertTrue(resultset.hasNext());

      final OResult item = resultset.next();
      Assertions.assertEquals(1000, (int) item.getProperty("count"));

    } finally {
      database.close();
    }
  }
}