/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.mongodbw;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * This class has been copied to avoid complex dependencies.
 */
public abstract class BaseGraphServerTest {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  protected static final String DB_PATH           = "./target/databases";

  protected static RID              root;
  private          ArcadeDBServer[] servers;

  @BeforeEach
  public void startTest() {
    LogManager.instance().info(this, "Starting test %s...", getClass().getName());

    deleteDatabaseFolders();

    new DatabaseFactory(getDatabasePath(0), PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        if (isPopulateDatabase()) {
          Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));

          VertexType v = database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);
          v.createProperty("id", Long.class);

          database.getSchema().createClassIndexes(true, VERTEX1_TYPE_NAME, new String[] { "id" });

          Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
          database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

          database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
          database.getSchema().createEdgeType(EDGE2_TYPE_NAME);

          database.getSchema().createDocumentType("Person");
        }
      }
    });

    if (isPopulateDatabase()) {
      final Database db = new DatabaseFactory(getDatabasePath(0), PaginatedFile.MODE.READ_WRITE).open();
      db.begin();
      try {
        final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
        v1.set("id", 0);
        v1.set("name", VERTEX1_TYPE_NAME);
        v1.save();

        final ModifiableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
        v2.set("name", VERTEX2_TYPE_NAME);
        v2.save();

        // CREATION OF EDGE PASSING PARAMS AS VARARGS
        ModifiableEdge e1 = (ModifiableEdge) v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
        Assertions.assertEquals(e1.getOut(), v1);
        Assertions.assertEquals(e1.getIn(), v2);

        final ModifiableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
        v3.set("name", "V3");
        v3.save();

        Map<String, Object> params = new HashMap<>();
        params.put("name", "E2");

        // CREATION OF EDGE PASSING PARAMS AS MAP
        ModifiableEdge e2 = (ModifiableEdge) v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
        Assertions.assertEquals(e2.getOut(), v2);
        Assertions.assertEquals(e2.getIn(), v3);

        ModifiableEdge e3 = (ModifiableEdge) v1.newEdge(EDGE2_TYPE_NAME, v3, true);
        Assertions.assertEquals(e3.getOut(), v1);
        Assertions.assertEquals(e3.getIn(), v3);

        db.commit();

        root = v1.getIdentity();

      } finally {
        db.close();
      }
    }

    startServers();
  }

  @AfterEach
  public void endTest() {
    LogManager.instance().info(this, "END OF THE TEST: Cleaning test %s...", getClass().getName());
    for (int i = servers.length - 1; i > -1; --i) {
      if (servers[i] != null)
        servers[i].stop();

      if (dropDatabases()) {
        final Database db = new DatabaseFactory("./target/databases" + i + "/" + getDatabaseName(), PaginatedFile.MODE.READ_WRITE)
            .open();
        db.drop();
      }
    }
  }

  protected void startServers() {
    final int totalServers = getServerCount();
    servers = new ArcadeDBServer[totalServers];

    int port = 2424;
    String serverURLs = "";
    for (int i = 0; i < totalServers; ++i) {
      if (i > 0)
        serverURLs += ",";
      serverURLs += "localhost:" + (port++);
    }

    for (int i = 0; i < totalServers; ++i) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + i);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverURLs);
      config.setValue(GlobalConfiguration.HA_ENABLED, getServerCount() > 1);

      servers[i] = new ArcadeDBServer(config);
      servers[i].start();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

//      if (serverId > 0) {
//        HttpURLConnection connection = null;
//        try {
//          connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/server").openConnection();
//
//          connection.setRequestMethod("POST");
//          connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
//
//          final String payload = "{\"add\":[\"Person\"],\"remove\":[\"Jay\"]}";
//
//          connection.setRequestMethod("POST");
//          connection.setDoOutput(true);
//
//          connection.connect();
//
//          PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
//          pw.write(payload);
//          pw.close();
//
//          final String response = readResponse(connection);
//
//          Assertions.assertEquals(200, connection.getResponseCode());
//          Assertions.assertEquals("OK", connection.getResponseMessage());
//
//          LogManager.instance().info(this, "Response: ", response);
//
//        } catch (IOException e) {
//          e.printStackTrace();
//        } finally {
//          connection.disconnect();
//        }
//      }
    }
  }

  protected void deleteDatabaseFolders() {
    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(getDatabasePath(i)));
  }

  protected boolean isPopulateDatabase() {
    return true;
  }

  protected ArcadeDBServer getServer(final int i) {
    return servers[i];
  }

  protected int getServerCount() {
    return 1;
  }

  protected boolean dropDatabases() {
    return true;
  }

  protected String getDatabaseName() {
    return "graph";
  }

  protected String getDatabasePath(final int serverId) {
    return DB_PATH + serverId + "/" + getDatabaseName();
  }

  protected String readResponse(final HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream();
    Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }
}