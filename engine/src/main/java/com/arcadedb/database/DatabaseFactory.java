package com.arcadedb.database;

import com.arcadedb.engine.PaginatedFile;

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
  private       boolean                                                    autoTransaction = false;
  private       Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks       = new HashMap<>();

  public DatabaseFactory(final String path, final PaginatedFile.MODE mode) {
    this.mode = mode;
    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  public Database acquire() {
    final DatabaseInternal db = new EmbeddedDatabase(databasePath, mode, callbacks);
    db.setAutoTransaction(autoTransaction);
    return db;
  }

  public void execute(final POperation operation) {
    if (operation == null)
      throw new IllegalArgumentException("Operation block is null");

    final Database db = acquire();
    try {
      db.transaction(new Database.PTransaction() {
        @Override
        public void execute(Database database) {
          operation.execute(database);
        }
      });
    } finally {
      db.close();
    }
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
