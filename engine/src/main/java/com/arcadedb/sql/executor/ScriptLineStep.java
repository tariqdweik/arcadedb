package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *         <p>
 *         This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  private final OInternalExecutionPlan plan;

  public ScriptLineStep(OInternalExecutionPlan nextPlan, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.plan = nextPlan;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    return plan.fetchNext(nRecords);
  }

  public boolean containsReturn() {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).containsReturn();
    }
    return false;
  }

  public OExecutionStepInternal executeUntilReturn(OCommandContext ctx) {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).executeUntilReturn();
    }
    throw new IllegalStateException();
  }
}
