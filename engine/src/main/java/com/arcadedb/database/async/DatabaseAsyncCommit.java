/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

public class DatabaseAsyncCommit extends DatabaseAsyncAbstractCallbackTask {
  public DatabaseAsyncCommit() {
  }

  @Override
  public String toString() {
    return "Commit";
  }
}
