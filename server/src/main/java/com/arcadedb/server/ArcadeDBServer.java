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
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ArcadeDBServer {
  private final ContextConfiguration configuration;

  private HAServer       haServer;
  private ServerSecurity security;
  private HttpServer     httpServer;

  private       ConcurrentMap<String, DatabaseInternal> databases          = new ConcurrentHashMap<>();
  private final String                                  serverName;
  private       List<TestCallback>                      testEventListeners = new ArrayList<>();
  private final boolean                                 testEnabled        = GlobalConfiguration.TEST.getValueAsBoolean();
  private final Map<String, ServerPlugin>               plugins            = new HashMap<>();

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
  }

  public static void main(final String[] args) throws IOException, InterruptedException {
    new ArcadeDBServer(new ContextConfiguration()).start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public void start() {
    lifecycleEvent(TestCallback.TYPE.SERVER_STARTING, null);

    log(this, Level.INFO, "Starting ArcadeDB Server...");

    loadDatabases();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    security = new ServerSecurity(this, "config");
    security.startService();

    httpServer = new HttpServer(this);
    httpServer.startService();

    final String registeredPlugins = GlobalConfiguration.SERVER_PLUGINS.getValueAsString();

    if (registeredPlugins != null && !registeredPlugins.isEmpty()) {
      final String[] pluginEntries = registeredPlugins.split(",");
      for (String p : pluginEntries) {
        try {
          final String[] pluginPair = p.split(":");

          final String pluginName = pluginPair[0];
          final String pluginClass = pluginPair.length > 1 ? pluginPair[1] : pluginPair[0];

          final Class<ServerPlugin> c = (Class<ServerPlugin>) Class.forName(pluginClass);
          final ServerPlugin pluginInstance = c.newInstance();
          pluginInstance.configure(this, configuration);

          pluginInstance.startService();

          plugins.put(pluginName, pluginInstance);

          log(this, Level.INFO, "- Plugin %s started", pluginName);

        } catch (Exception e) {
          throw new ServerException("Error on loading plugin from class '" + p + ";", e);
        }
      }
    }

    log(this, Level.INFO, "ArcadeDB Server started (CPUs=%d MAXRAM=%s)", Runtime.getRuntime().availableProcessors(),
        FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));

    lifecycleEvent(TestCallback.TYPE.SERVER_UP, null);
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
    lifecycleEvent(TestCallback.TYPE.SERVER_SHUTTING_DOWN, null);

    log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    for (Map.Entry<String, ServerPlugin> pEntry : plugins.entrySet()) {
      log(this, Level.INFO, "- Stop %s plugin", pEntry.getKey());
      try {
        pEntry.getValue().stopService();
      } catch (Exception e) {
        log(this, Level.SEVERE, "Error on halting %s plugin (error=%s)", pEntry.getKey(), e);
      }
    }

    if (haServer != null)
      haServer.stopService();

    if (httpServer != null)
      httpServer.stopService();

    if (security != null)
      security.stopService();

    for (Database db : databases.values())
      db.close();

    log(this, Level.INFO, "ArcadeDB Server is down");

    lifecycleEvent(TestCallback.TYPE.SERVER_DOWN, null);
  }

  public Database getDatabase(final String databaseName) {
    return getDatabase(databaseName, false);
  }

  public Database getOrCreateDatabase(final String databaseName) {
    return getDatabase(databaseName, true);
  }

  public DatabaseInternal createDatabase(final String databaseName) {
    DatabaseInternal db = databases.get(databaseName);
    if (db != null)
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    final DatabaseFactory factory = new DatabaseFactory(
        configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName,
        PaginatedFile.MODE.READ_WRITE).setAutoTransaction(true);

    if (factory.exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      factory.setWALFileFactory(new ReplicatedWALFileFactory());

    db = factory.create();

    // FORCE THREAD AFFINITY TO REDUCE CONFLICTS
    for (DocumentType t : db.getSchema().getTypes()) {
      t.setSyncSelectionStrategy(new ThreadAffinityBucketSelectionStrategy());
    }

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

    final DatabaseInternal oldDb = databases.putIfAbsent(databaseName, db);

    if (oldDb != null)
      db = oldDb;

    return db;
  }

  public Set<String> getDatabaseNames() {
    return databases.keySet();
  }

  public void log(final Object requester, final Level level, final String message, final Object... args) {
    LogManager.instance().log(requester, level, "<" + getServerName() + "> " + message, null, false, args);
  }

  public void removeDatabase(final String databaseName) {
    databases.remove(databaseName);
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

  public void registerTestEventListener(final TestCallback callback) {
    testEventListeners.add(callback);
  }

  public void lifecycleEvent(final TestCallback.TYPE type, final Object object) {
    if (testEnabled)
      for (TestCallback c : testEventListeners)
        c.onEvent(type, object, this);
  }

  public String getRootPath() {
    return new File(".").getAbsolutePath();
  }

  @Override
  public String toString() {
    return getServerName();
  }

  private synchronized Database getDatabase(final String databaseName, final boolean createIfNotExists) {
    DatabaseInternal db = databases.get(databaseName);
    if (db == null) {
      final DatabaseFactory factory = new DatabaseFactory(
          configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName,
          PaginatedFile.MODE.READ_WRITE).setAutoTransaction(true);

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        factory.setWALFileFactory(new ReplicatedWALFileFactory());

      if (createIfNotExists)
        db = factory.exists() ? factory.open() : factory.create();
      else
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
}
