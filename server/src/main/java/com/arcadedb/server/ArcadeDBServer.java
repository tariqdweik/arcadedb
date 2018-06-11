/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.ha.ReplicatedWALFileFactory;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.LogManager;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ArcadeDBServer {
  private final ContextConfiguration configuration;

  private final HttpServer httpServer;
  private       HAServer   haServer;

  private       ConcurrentMap<String, DatabaseInternal> databases = new ConcurrentHashMap<>();
  private final String                                  serverName;
  private       ServerSecurity                          security;

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    this.httpServer = new HttpServer(this);
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
  }

  public static void main(final String[] args) throws IOException, InterruptedException {
    new ArcadeDBServer(new ContextConfiguration()).start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public void start() throws IOException, InterruptedException {
    log(this, Level.INFO, "Starting ArcadeDB Server...");

    loadDatabases();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.connect();
    }

    security = new ServerSecurity(this, "config");

    httpServer.start();

    log(this, Level.INFO, "ArcadeDB Server started");
  }

  private void loadDatabases() {
    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      databaseDir.mkdirs();
      return;
    }

    if (!databaseDir.isDirectory())
      throw new ConfigurationException("Configured database directory '" + databaseDir + "' is not a directory on file system");

    final File[] databaseDirectories = databaseDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    for (File f : databaseDirectories)
      getDatabase(f.getName());
  }

  public void stop() {
    log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    if (security != null)
      security.close();

    if (haServer != null)
      haServer.close();

    if (httpServer != null)
      httpServer.stop();

    for (Database db : databases.values())
      db.close();

    log(this, Level.INFO, "ArcadeDB Server is down");
  }

  public synchronized Database getDatabase(final String databaseName) {
    DatabaseInternal db = databases.get(databaseName);
    if (db == null) {
      final DatabaseFactory factory = new DatabaseFactory(
          configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName,
          PaginatedFile.MODE.READ_WRITE).setAutoTransaction(true);

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        factory.setWALFileFactory(new ReplicatedWALFileFactory());

      db = factory.open();

      // FORCE THREAD AFFINITY TO REDUCE CONFLICTS
      for (DocumentType t : db.getSchema().getTypes()) {
        t.setSyncSelectionStrategy(new ThreadAffinityBucketSelectionStrategy());
      }

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

      final DatabaseInternal oldDb = databases.putIfAbsent(databaseName, db);

      if (oldDb != null)
        db = oldDb;
    }

    return new ServerDatabaseProxy(db);
  }

  public Set<String> getDatabaseNames() {
    return databases.keySet();
  }

  public void log(final Object requester, final Level level, final String message, final Object... args) {
    LogManager.instance().log(requester, level, "<" + getServerName() + "> " + message, null, false, args);
  }

  public String getServerName() {
    return serverName;
  }

  public HAServer getHA() {
    return haServer;
  }

  public ServerSecurity getSecurity() {
    return security;
  }
}
