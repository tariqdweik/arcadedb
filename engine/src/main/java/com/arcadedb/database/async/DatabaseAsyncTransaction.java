/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.Database;

public class DatabaseAsyncTransaction implements DatabaseAsyncTask {
  public final Database.Transaction tx;
  public final int                  retries;

  public DatabaseAsyncTransaction(final Database.Transaction tx, final int retries) {
    this.tx = tx;
    this.retries = retries;
  }

  @Override
  public String toString() {
    return "Transaction(" + tx + ")";
  }
}
