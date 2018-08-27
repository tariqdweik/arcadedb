/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.SchemaImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class DatabaseFactory implements AutoCloseable {
  public interface DatabaseOperation {
    void execute(Database database);
  }

  private final ContextConfiguration                                       contextConfiguration = new ContextConfiguration();
  private final String                                                     databasePath;
  private       boolean                                                    autoTransaction      = false;
  private       Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks            = new HashMap<>();

  public DatabaseFactory(final String path) {
    if (path == null || path.isEmpty())
      throw new IllegalArgumentException("Missing path");

    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  @Override
  public synchronized void close() {
    callbacks.clear();
  }

  public boolean exists() {
    return new File(databasePath + "/" + SchemaImpl.SCHEMA_FILE_NAME).exists();
  }

  public Database open() {
    return open(PaginatedFile.MODE.READ_WRITE);
  }

  public synchronized Database open(final PaginatedFile.MODE mode) {
    final EmbeddedDatabase database = new EmbeddedDatabase(databasePath, mode, contextConfiguration, callbacks);
    database.setAutoTransaction(autoTransaction);
    database.open();
    return database;
  }

  public synchronized Database create() {
    final EmbeddedDatabase database = new EmbeddedDatabase(databasePath, PaginatedFile.MODE.READ_WRITE, contextConfiguration, callbacks);
    database.setAutoTransaction(autoTransaction);
    database.create();
    return database;
  }

  public DatabaseFactory setAutoTransaction(final boolean enabled) {
    autoTransaction = enabled;
    return this;
  }

  public ContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  /**
   * Test only API
   */
  public void registerCallback(final DatabaseInternal.CALLBACK_EVENT event, Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks == null) {
      callbacks = new ArrayList<Callable<Void>>();
      this.callbacks.put(event, callbacks);
    }
    callbacks.add(callback);
  }
}
