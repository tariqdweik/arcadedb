/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ThreadAffinityBucketSelectionStrategy;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ArcadeDBServer {
  private final ContextConfiguration configuration;

  private final HttpServer httpServer;
  private final HAServer   haServer;

  private ConcurrentMap<String, Database> databases = new ConcurrentHashMap<>();

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    this.haServer = new HAServer(configuration);
    this.httpServer = new HttpServer(this);
  }

  public static void main(final String[] args) throws IOException {
    new ArcadeDBServer(new ContextConfiguration()).start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public void start() throws IOException {
    LogManager.instance().info(this, "Starting ArcadeDB Server...");

    haServer.start();
    httpServer.start();

    LogManager.instance().info(this, "ArcadeDB Server started");
  }

  public void stop() {
    LogManager.instance().info(this, "Shutting down ArcadeDB Server...");

    haServer.stop();
    httpServer.stop();

    for (Database db : databases.values())
      db.close();

    LogManager.instance().info(this, "ArcadeDB Server is down");
  }

  public synchronized Database getDatabase(final String databaseName) {
    Database db = databases.get(databaseName);
    if (db == null) {
      db = new DatabaseFactory(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName,
          PaginatedFile.MODE.READ_WRITE).setAutoTransaction(true).acquire();

      // FORCE THREAD AFFINITY TO REDUCE CONFLICTS
      for (DocumentType t : db.getSchema().getTypes()) {
        t.setSyncSelectionStrategy(new ThreadAffinityBucketSelectionStrategy());
      }

      final Database oldDb = databases.putIfAbsent(databaseName, db);

      if (oldDb != null)
        db = oldDb;
    }
    return db;
  }

  public HAServer getZKServer() {
    return haServer;
  }
}
