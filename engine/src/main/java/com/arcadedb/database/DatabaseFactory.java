/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.WALFileFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class DatabaseFactory {
  public interface POperation {
    void execute(Database database);
  }

  private final PaginatedFile.MODE                                         mode;
  private final String                                                     databasePath;
  private       WALFileFactory                                             walFileFactory;
  private       boolean                                                    autoTransaction = false;
  private       Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks       = new HashMap<>();

  public DatabaseFactory(final String path, final PaginatedFile.MODE mode) {
    this.mode = mode;
    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  public EmbeddedDatabase open() {
    final EmbeddedDatabase db = new EmbeddedDatabase(databasePath, mode, callbacks, walFileFactory);
    db.setAutoTransaction(autoTransaction);
    return db;
  }

  public void execute(final POperation operation) {
    if (operation == null)
      throw new IllegalArgumentException("Operation block is null");

    final Database db = open();
    try {
      db.transaction(new Database.Transaction() {
        @Override
        public void execute(Database database) {
          operation.execute(database);
        }
      });
    } finally {
      db.close();
    }
  }

  public DatabaseFactory setWALFileFactory(final WALFileFactory walFileFactory) {
    this.walFileFactory = walFileFactory;
    return this;
  }

  public DatabaseFactory setAutoTransaction(final boolean enabled) {
    autoTransaction = enabled;
    return this;
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
