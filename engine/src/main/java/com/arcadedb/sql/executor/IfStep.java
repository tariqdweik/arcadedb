package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.BooleanExpression;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class IfStep extends AbstractExecutionStep {
  BooleanExpression    condition;
  OScriptExecutionPlan positivePlan;
  OScriptExecutionPlan negativePlan;

  Boolean conditionMet = null;

  public IfStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    init(ctx);
    if (conditionMet) {
      return positivePlan.fetchNext(nRecords);
    } else if (negativePlan != null) {
      return negativePlan.fetchNext(nRecords);
    } else {
      return new OInternalResultSet();
    }
  }

  protected void init(OCommandContext ctx) {
    if (conditionMet == null) {
      conditionMet = condition.evaluate((OResult) null, ctx);
    }
  }

  public BooleanExpression getCondition() {
    return condition;
  }

  public void setCondition(BooleanExpression condition) {
    this.condition = condition;
  }

  public OScriptExecutionPlan getPositivePlan() {
    return positivePlan;
  }

  public void setPositivePlan(OScriptExecutionPlan positivePlan) {
    this.positivePlan = positivePlan;
  }

  public OScriptExecutionPlan getNegativePlan() {
    return negativePlan;
  }

  public void setNegativePlan(OScriptExecutionPlan negativePlan) {
    this.negativePlan = negativePlan;
  }
}
