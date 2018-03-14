package com.arcadedb.database;

import com.arcadedb.engine.PFile;

public class PDatabaseFactory {
  public interface POperation {
    void execute(PDatabase database);
  }

  private final PFile.MODE mode;
  private final String     databasePath;
  private boolean multiThread     = true;
  private boolean autoTransaction = false;
  private boolean parallel        = false;

  public PDatabaseFactory(final String path, final PFile.MODE mode) {
    this.mode = mode;
    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  public PDatabase acquire() {
    PDatabase db = parallel ?
        new PDatabaseParallel(databasePath, mode, multiThread) :
        new PDatabaseImpl(databasePath, mode, multiThread);
    db.setAutoTransaction(autoTransaction);
    return db;
  }

  public PDatabaseFactory useParallel(final boolean value) {
    this.parallel = value;
    return this;
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
