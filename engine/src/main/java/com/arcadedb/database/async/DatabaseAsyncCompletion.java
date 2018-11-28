/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;

public class DatabaseAsyncCompletion extends DatabaseAsyncAbstractCallbackTask {
  public DatabaseAsyncCompletion() {
  }

  @Override
  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    try {
      database.commit();
      async.onOk();
    } catch (Exception e) {
      async.onError(e);
    }
    database.begin();
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "Completion";
  }
}
