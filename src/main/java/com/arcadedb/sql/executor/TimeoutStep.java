package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.OTimeout;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class TimeoutStep extends AbstractExecutionStep {
  private final OTimeout timeout;

  private Long expiryTime;

  public TimeoutStep(OTimeout timeout, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    if (this.expiryTime == null) {
      this.expiryTime = System.currentTimeMillis() + timeout.getVal().longValue();
    }
    if (System.currentTimeMillis() > expiryTime) {
      return fail();
    }
    return getPrev().get().syncPull(ctx, nRecords);//TODO do it more granular
  }

  private OResultSet fail() {
    this.timedOut = true;
    sendTimeout();
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return new OInternalResultSet();
    } else {
      throw new PTimeoutException("Timeout expired");
    }
  }

}
