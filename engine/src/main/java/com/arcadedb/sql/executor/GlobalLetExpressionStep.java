/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.Identifier;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class GlobalLetExpressionStep extends AbstractExecutionStep {
  private final Identifier varname;
  private final Expression expression;

  boolean executed = false;

  public GlobalLetExpressionStep(Identifier varName, Expression expression, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    calculate(ctx);
    return new InternalResultSet();
  }

  private void calculate(CommandContext ctx) {
    if (executed) {
      return;
    }
    Object value = expression.execute((Result) null, ctx);
    ctx.setVariable(varname.getStringValue(), value);
    executed = true;
  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (once)\n" +
        spaces + "  " + varname + " = " + expression;
  }
}
