/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ArcadeDBServer {
  public static final String CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";
  private final        ContextConfiguration configuration;
  private final        boolean              fileConfiguration;

  private HAServer       haServer;
  private ServerSecurity security;
  private HttpServer     httpServer;

  private          ConcurrentMap<String, DatabaseInternal> databases          = new ConcurrentHashMap<>();
  private final    String                                  serverName;
  private          List<TestCallback>                      testEventListeners = new ArrayList<>();
  private final    boolean                                 testEnabled;
  private final    Map<String, ServerPlugin>               plugins            = new HashMap<>();
  private volatile boolean                                 started            = false;
  private          ServerMetrics                           serverMetrics      = new NoServerMetrics();

  public ArcadeDBServer() {
    this.configuration = new ContextConfiguration();
    this.fileConfiguration = true;
    loadConfiguration();

    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
  }

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.fileConfiguration = false;
    this.configuration = configuration;
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
  }

  public static void main(final String[] args) {
    new ArcadeDBServer().start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public synchronized void start() {
    LogManager.instance().setContext(getServerName());

    if (started)
      return;

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_STARTING, null);
    } catch (Exception e) {
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    log(this, Level.INFO, "Starting ArcadeDB Server...");

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) {
      serverMetrics.stop();
      serverMetrics = new JMXServerMetrics();
      log(this, Level.INFO, "- JMX Metrics Started...");
    }

    security = new ServerSecurity(this, "config");
    security.startService();

    loadDatabases();

    httpServer = new HttpServer(this);
    httpServer.startService();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    final String registeredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);

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

    started = true;

    log(this, Level.INFO, "ArcadeDB Server started (CPUs=%d MAXRAM=%s)", Runtime.getRuntime().availableProcessors(),
        FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_UP, null);
    } catch (Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }
  }

  public synchronized void stop() {
    if (!started)
      return;

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_SHUTTING_DOWN, null);
    } catch (Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    started = false;

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
    databases.clear();

    log(this, Level.INFO, "- Stop JMX Metrics");
    serverMetrics.stop();
    serverMetrics = new NoServerMetrics();

    log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_DOWN, null);
    } catch (Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
  }

  public ServerMetrics getServerMetrics() {
    return serverMetrics;
  }

  public Database getDatabase(final String databaseName) {
    return getDatabase(databaseName, false);
  }

  public Database getOrCreateDatabase(final String databaseName) {
    return getDatabase(databaseName, true);
  }

  public boolean isStarted() {
    return started;
  }

  public synchronized boolean existsDatabase(final String databaseName) {
    return databases.containsKey(databaseName);
  }

  public synchronized DatabaseInternal createDatabase(final String databaseName) {
    DatabaseInternal db = databases.get(databaseName);
    if (db != null)
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    final DatabaseFactory factory = new DatabaseFactory(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName)
        .setAutoTransaction(true);

    if (factory.exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    db = (DatabaseInternal) factory.create();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

    databases.put(databaseName, db);

    return db;
  }

  public Set<String> getDatabaseNames() {
    return databases.keySet();
  }

  public void log(final Object requester, final Level level, final String message, final Object... args) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);

    LogManager.instance().log(requester, level, message, null, false, args);
  }

  public synchronized void removeDatabase(final String databaseName) {
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

  public void lifecycleEvent(final TestCallback.TYPE type, final Object object) throws Exception {
    if (testEnabled)
      for (TestCallback c : testEventListeners)
        c.onEvent(type, object, this);
  }

  public String getRootPath() {
    return new File(configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH)).getAbsolutePath();
  }

  public HttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public String toString() {
    return getServerName();
  }

  private synchronized Database getDatabase(final String databaseName, final boolean createIfNotExists) {
    DatabaseInternal db = databases.get(databaseName);
    if (db == null || !db.isOpen()) {

      final DatabaseFactory factory = new DatabaseFactory(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName)
          .setAutoTransaction(true);

      if (createIfNotExists)
        db = (DatabaseInternal) (factory.exists() ? factory.open() : factory.create());
      else
        db = (DatabaseInternal) factory.open();

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

      databases.put(databaseName, db);
    }

    return db;
  }

  private void loadDatabases() {
    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      databaseDir.mkdirs();
    } else {
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

    final String defaultDatabases = configuration.getValueAsString(GlobalConfiguration.SERVER_DEFAULT_DATABASES);
    if (defaultDatabases != null && !defaultDatabases.isEmpty()) {
      // CREATE DEFAULT DATABASES
      final String[] dbs = defaultDatabases.split(";");
      for (String db : dbs) {
        final int credentialPos = db.indexOf('[');
        if (credentialPos < 0) {
          LogManager.instance().warn(this, "Error in default databases format: '%s'", defaultDatabases);
          break;
        }

        final String dbName = db.substring(0, credentialPos);
        final String credentials = db.substring(credentialPos + 1, db.length() - 1);

        final String[] credentialPairs = credentials.split(",");
        for (String credential : credentialPairs) {

          final int passwordSeparator = credential.indexOf(":");

          if (passwordSeparator < 0) {
            if (!getSecurity().existsUser(credential)) {
              LogManager.instance().warn(this, "Cannot create user '%s' accessing to database '%s' because the user does not exists", credential, dbName);
              continue;
            }
          } else {
            final String userName = credential.substring(0, passwordSeparator);
            final String userPassword = credential.substring(passwordSeparator + 1);

            if (getSecurity().existsUser(userName)) {
              // EXISTING USER: CHECK CREDENTIALS
              try {
                final ServerSecurity.ServerUser user = getSecurity().authenticate(userName, userPassword);
                if (!user.databaseBlackList && !user.databases.contains(dbName)) {
                  // UPDATE DB LIST
                  user.databases.add(dbName);
                  try {
                    getSecurity().saveConfiguration();
                  } catch (IOException e) {
                    LogManager.instance().error(this, "Cannot create database '%s' because security configuration cannot be saved", e, dbName);
                    continue;
                  }
                }

              } catch (ServerSecurityException e) {
                LogManager.instance().warn(this, "Cannot create database '%s' because the user '%s' already exists with different password", dbName, userName);
                continue;
              }
            } else {
              // CREATE A NEW USER
              try {
                getSecurity().createUser(userName, userPassword, false, Collections.singletonList(dbName));
              } catch (IOException e) {
                LogManager.instance().error(this, "Cannot create database '%s' because the new user '%s' cannot be saved", e, dbName, userName);
                continue;
              }
            }
          }
        }

        // CREATE THE DATABASE
        if (!existsDatabase(dbName)) {
          LogManager.instance().info(this, "Creating default database '%s'...", dbName);
          createDatabase(dbName);
        }
      }
    }
  }

  private void loadConfiguration() {
    final File file = new File(CONFIG_SERVER_CONFIGURATION_FILENAME);
    if (file.exists()) {
      try {
        final String content = FileUtils.readFileAsString(file, "UTF8");
        configuration.reset();
        configuration.fromJSON(content);

      } catch (IOException e) {
        LogManager.instance().error(this, "Error on loading configuration from file '%s'", e, file);
      }
    }
  }

  private void saveConfiguration() {
    final File file = new File(CONFIG_SERVER_CONFIGURATION_FILENAME);
    try {
      FileUtils.writeFile(file, configuration.toJSON());
    } catch (IOException e) {
      LogManager.instance().error(this, "Error on saving configuration to file '%s'", e, file);
    }
  }
}
