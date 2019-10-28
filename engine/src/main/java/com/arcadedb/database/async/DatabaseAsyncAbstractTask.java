/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
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
