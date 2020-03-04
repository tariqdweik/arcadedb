/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;

public interface DatabaseAsyncTask {
  void execute(DatabaseAsyncExecutor.AsyncThread async, DatabaseInternal database);

  void completed();

  boolean requiresActiveTx();
}
