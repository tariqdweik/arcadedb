package com.arcadedb.sql.executor;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DeleteExecutionPlan extends UpdateExecutionPlan {

  public DeleteExecutionPlan(CommandContext ctx) {
    super(ctx);
  }

  @Override public Result toResult() {
    ResultInternal res = (ResultInternal) super.toResult();
    res.setProperty("type", "DeleteExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}

