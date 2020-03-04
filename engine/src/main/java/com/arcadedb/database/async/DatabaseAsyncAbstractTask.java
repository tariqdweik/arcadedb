/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

public abstract class DatabaseAsyncAbstractTask implements DatabaseAsyncTask {
  public void completed() {
  }

  @Override
  public boolean requiresActiveTx() {
    return true;
  }
}
