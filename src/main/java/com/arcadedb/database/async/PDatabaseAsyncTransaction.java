package com.arcadedb.database.async;

import com.arcadedb.database.PDatabase;

public class PDatabaseAsyncTransaction extends PDatabaseAsyncCommand {
  public final PDatabase.PTransaction tx;

  public PDatabaseAsyncTransaction(final PDatabase.PTransaction tx) {
    this.tx = tx;
  }
}
