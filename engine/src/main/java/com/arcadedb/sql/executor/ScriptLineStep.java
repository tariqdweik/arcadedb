package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *         <p>
 *         This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  private final InternalExecutionPlan plan;

  public ScriptLineStep(InternalExecutionPlan nextPlan, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.plan = nextPlan;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    return plan.fetchNext(nRecords);
  }

  public boolean containsReturn() {
    if (plan instanceof ScriptExecutionPlan) {
      return ((ScriptExecutionPlan) plan).containsReturn();
    }
    return false;
  }

  public ExecutionStepInternal executeUntilReturn(CommandContext ctx) {
    if (plan instanceof ScriptExecutionPlan) {
      return ((ScriptExecutionPlan) plan).executeUntilReturn();
    }
    throw new IllegalStateException();
  }
}
