package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.BooleanExpression;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class IfStep extends AbstractExecutionStep {
  BooleanExpression   condition;
  ScriptExecutionPlan positivePlan;
  ScriptExecutionPlan negativePlan;

  Boolean conditionMet = null;

  public IfStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    init(ctx);
    if (conditionMet) {
      return positivePlan.fetchNext(nRecords);
    } else if (negativePlan != null) {
      return negativePlan.fetchNext(nRecords);
    } else {
      return new InternalResultSet();
    }
  }

  protected void init(CommandContext ctx) {
    if (conditionMet == null) {
      conditionMet = condition.evaluate((Result) null, ctx);
    }
  }

  public BooleanExpression getCondition() {
    return condition;
  }

  public void setCondition(BooleanExpression condition) {
    this.condition = condition;
  }

  public ScriptExecutionPlan getPositivePlan() {
    return positivePlan;
  }

  public void setPositivePlan(ScriptExecutionPlan positivePlan) {
    this.positivePlan = positivePlan;
  }

  public ScriptExecutionPlan getNegativePlan() {
    return negativePlan;
  }

  public void setNegativePlan(ScriptExecutionPlan negativePlan) {
    this.negativePlan = negativePlan;
  }
}
