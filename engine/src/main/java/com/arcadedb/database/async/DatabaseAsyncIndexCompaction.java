/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.index.Index;

public class DatabaseAsyncIndexCompaction implements DatabaseAsyncTask {
  public final Index index;

  public DatabaseAsyncIndexCompaction(final Index index) {
    this.index = index;
  }

  @Override
  public String toString() {
    return "IndexCompaction(" + index.getName() + ")";
  }
}
