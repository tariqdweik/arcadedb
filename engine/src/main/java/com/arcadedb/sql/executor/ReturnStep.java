package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class ReturnStep extends AbstractExecutionStep {
  public ReturnStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    return null;
  }

}
