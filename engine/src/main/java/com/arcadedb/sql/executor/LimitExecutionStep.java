/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Limit;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class LimitExecutionStep extends AbstractExecutionStep {
  private final Limit limit;

  int loaded = 0;

  public LimitExecutionStep(Limit limit, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.limit = limit;
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    int limitVal = limit.getValue(ctx);
    if (limitVal == -1) {
      return getPrev().get().syncPull(ctx, nRecords);
    }
    if (limitVal <= loaded) {
      return new InternalResultSet();
    }
    int nextBlockSize = Math.min(nRecords, limitVal - loaded);
    ResultSet result = prev.get().syncPull(ctx, nextBlockSize);
    loaded += nextBlockSize;
    return result;
  }

  @Override public void sendTimeout() {

  }

  @Override public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ LIMIT (" + limit.toString() + ")";
  }

}
