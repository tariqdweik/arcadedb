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
      Assertions.assertEquals(record.getProperty("name"), "V1");

    } finally {
      database.close();
    }
  }
}