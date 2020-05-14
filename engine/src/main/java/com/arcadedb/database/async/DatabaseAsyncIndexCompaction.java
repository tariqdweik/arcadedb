/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncIndexCompaction extends DatabaseAsyncAbstractTask {
  public final IndexInternal index;

  public DatabaseAsyncIndexCompaction(final IndexInternal index) {
    this.index = index;
  }

  @Override
  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    if (database.isTransactionActive())
      database.commit();

    try {
      ((EmbeddedDatabase) database.getEmbedded()).indexCompactions.incrementAndGet();
      index.compact();
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on executing compaction of index '%s'", e, index.getName());
    }
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "IndexCompaction(" + index.getName() + ")";
  }

}
