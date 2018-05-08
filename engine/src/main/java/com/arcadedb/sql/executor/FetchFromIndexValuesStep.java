package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.PIndex;

/**
 * Created by luigidellaquila on 02/08/16.
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  private boolean asc;

  public FetchFromIndexValuesStep(PIndex index, boolean asc, CommandContext ctx, boolean profilingEnabled) {
    super(index, null, null, ctx, profilingEnabled);
    this.asc = asc;
  }

  @Override
  protected boolean isOrderAsc() {
    return asc;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (isOrderAsc()) {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VAUES ASC " + index.getName();
    } else {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VAUES DESC " + index.getName();
    }
  }

  @Override
  public Result serialize() {
    ResultInternal result = (ResultInternal) super.serialize();
    result.setProperty("asc", asc);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      super.deserialize(fromResult);
      this.asc = fromResult.getProperty("asc");
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}