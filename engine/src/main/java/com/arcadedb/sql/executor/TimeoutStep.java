/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Timeout;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class TimeoutStep extends AbstractExecutionStep {
  private final Timeout timeout;

  private Long expiryTime;

  public TimeoutStep(Timeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (this.expiryTime == null) {
      this.expiryTime = System.currentTimeMillis() + timeout.getVal().longValue();
    }
    if (System.currentTimeMillis() > expiryTime) {
      return fail();
    }
    return getPrev().get().syncPull(ctx, nRecords);//TODO do it more granular
  }

  private ResultSet fail() {
    this.timedOut = true;
    sendTimeout();
    if (Timeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return new InternalResultSet();
    } else {
      throw new TimeoutException("Timeout expired");
    }
  }

}
