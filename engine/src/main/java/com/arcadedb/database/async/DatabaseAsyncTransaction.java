package com.arcadedb.database.async;

import com.arcadedb.database.Database;

public class DatabaseAsyncTransaction extends DatabaseAsyncCommand {
  public final Database.PTransaction tx;
  public final int                   retries;

  public DatabaseAsyncTransaction(final Database.PTransaction tx, final int retries) {
    this.tx = tx;
    this.retries = retries;
  }
}
