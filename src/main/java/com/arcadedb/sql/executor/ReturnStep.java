package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class ReturnStep extends AbstractExecutionStep {
  public ReturnStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    return null;
  }

}
