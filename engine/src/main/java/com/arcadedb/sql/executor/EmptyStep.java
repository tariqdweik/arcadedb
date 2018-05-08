/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyStep extends AbstractExecutionStep {
  public EmptyStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    InternalResultSet result = new InternalResultSet();
    return result;
  }

  public ExecutionStep copy(CommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  public boolean canBeCached() {
    return false;
    // DON'T TOUCH!
    // This step is there most of the cases because the query was early optimized based on DATA, eg. an empty cluster,
    // so this execution plan cannot be cached!!!
  }

}
