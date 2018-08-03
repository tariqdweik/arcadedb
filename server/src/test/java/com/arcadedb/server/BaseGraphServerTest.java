/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * This class has been copied under Console project to avoid complex dependencies.
 */
public abstract class BaseGraphServerTest {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  protected static final String DB_PATH           = "./target/databases";

  protected static RID              root;
  private          ArcadeDBServer[] servers;

  protected BaseGraphServerTest() {
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @BeforeEach
  public void startTest() {
    checkArcadeIsTotallyDown();

    LogManager.instance().info(this, "Starting test %s...", getClass().getName());

    for (int i = 0; i < getServerCount(); ++i)
      FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/databases" + i));
    FileUtils.deleteRecursively(new File(GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString() + "/replication"));

    new DatabaseFactory(getDatabasePath(0), PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.DatabaseOperation() {
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
    try {
      LogManager.instance().info(this, "END OF THE TEST: Check DBS are identical...");
      checkDatabasesAreIdentical();
    } finally {
      LogManager.instance().info(this, "END OF THE TEST: Cleaning test %s...", getClass().getName());
      if (servers != null)
        for (int i = servers.length - 1; i > -1; --i) {
          if (servers[i] != null)
            servers[i].stop();

          if (dropDatabasesAtTheEnd()) {
            final DatabaseFactory factory = new DatabaseFactory("./target/databases" + i + "/" + getDatabaseName(), PaginatedFile.MODE.READ_WRITE);

            if (factory.exists()) {
              final Database db = factory.open();
              db.drop();
            }
          }
        }

      checkArcadeIsTotallyDown();

      GlobalConfiguration.TEST.setValue(false);
    }
  }

  protected void checkArcadeIsTotallyDown() {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final PrintWriter output = new PrintWriter(new BufferedOutputStream(os));
    new Exception().printStackTrace(output);
    output.flush();
    final String out = os.toString();
    Assertions.assertFalse(out.contains("ArcadeDB"), "Some thread is still up & running: \n" + out);
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
      onBeforeStarting(servers[i]);
      servers[i].start();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }
  }

  protected void onBeforeStarting(ArcadeDBServer server) {
  }

  protected boolean isPopulateDatabase() {
    return true;
  }

  protected ArcadeDBServer getServer(final int i) {
    return servers[i];
  }

  protected Database getServerDatabase(final int i, final String name) {
    return servers[i].getDatabase(name);
  }

  protected ArcadeDBServer getServer(final String name) {
    for (ArcadeDBServer s : servers) {
      if (s.getServerName().equals(name))
        return s;
    }
    return null;
  }

  protected int getServerCount() {
    return 1;
  }

  protected boolean dropDatabasesAtTheEnd() {
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

  protected void executeAsynchronously(final Callable callback) {
    final Timer task = new Timer();
    task.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          callback.call();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, 1);
  }

  protected ArcadeDBServer getLeaderServer() {
    for (int i = 0; i < getServerCount(); ++i)
      if (getServer(i).isStarted()) {
        final ArcadeDBServer onlineServer = getServer(i);
        final String leaderName = onlineServer.getHA().getLeaderName();
        return getServer(leaderName);
      }
    return null;
  }

  protected boolean areAllServersOnline() {
    final int onlineReplicas = getLeaderServer().getHA().getOnlineReplicas();
    if (1 + onlineReplicas < getServerCount()) {
      // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
      LogManager.instance().info(this, "TEST: Not all the servers are ONLINE (%d), skip this crash...", onlineReplicas);
      getLeaderServer().getHA().printClusterConfiguration();
      return false;
    }
    return true;
  }

  protected int[] getServerToCheck() {
    final int[] result = new int[getServerCount()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  protected void checkDatabasesAreIdentical() {
    final int[] servers2Check = getServerToCheck();

    for (int i = 1; i < servers2Check.length; ++i) {
      final Database db1 = getServerDatabase(servers2Check[0], getDatabaseName());
      final Database db2 = getServerDatabase(servers2Check[i], getDatabaseName());

      LogManager.instance().info(this, "TEST: Comparing databases '%s' and '%s' are identical...", db1, db2);
      new DatabaseComparator().compare(db1, db2);
    }
  }
}