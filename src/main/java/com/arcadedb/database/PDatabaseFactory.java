package com.arcadedb.database;

import com.arcadedb.engine.PPaginatedFile;

public class PDatabaseFactory {
  public interface POperation {
    void execute(PDatabase database);
  }

  private final PPaginatedFile.MODE mode;
  private final String              databasePath;
  private boolean multiThread     = true;
  private boolean autoTransaction = false;

  public PDatabaseFactory(final String path, final PPaginatedFile.MODE mode) {
    this.mode = mode;
    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  public PDatabase acquire() {
    final PDatabase db = new PDatabaseImpl(databasePath, mode, multiThread);
    db.setAutoTransaction(autoTransaction);
    return db;
  }

  public void execute(final POperation operation) {
    if (operation == null)
      throw new IllegalArgumentException("Operation block is null");

    final PDatabase db = acquire();
    try {
      db.transaction(new PDatabase.PTransaction() {
        @Override
        public void execute(PDatabase database) {
          operation.execute(database);
        }
      });
    } finally {
      db.close();
    }
  }

  public boolean isMultiThread() {
    return multiThread;
  }

  public PDatabaseFactory setMultiThread(final boolean multiThread) {
    this.multiThread = multiThread;
    return this;
  }

  public PDatabaseFactory setAutoTransaction(final boolean enabled) {
    autoTransaction = enabled;
    return this;
  }
}
