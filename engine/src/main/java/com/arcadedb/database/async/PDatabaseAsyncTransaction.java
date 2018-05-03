package com.arcadedb.database.async;

import com.arcadedb.database.PDatabase;

public class PDatabaseAsyncTransaction extends PDatabaseAsyncCommand {
  public final PDatabase.PTransaction tx;
  public final int                    retries;

  public PDatabaseAsyncTransaction(final PDatabase.PTransaction tx, final int retries) {
    this.tx = tx;
    this.retries = retries;
  }
}
