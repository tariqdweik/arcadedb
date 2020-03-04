/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.parser.ODDLStatement;

import java.util.Collections;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODDLExecutionPlan implements InternalExecutionPlan {

  private final ODDLStatement statement;
  CommandContext ctx;

  boolean executed = false;

  public ODDLExecutionPlan(CommandContext ctx, ODDLStatement stm) {
    this.ctx = ctx;
    this.statement = stm;
  }

  @Override
  public void close() {

  }

  @Override
  public ResultSet fetchNext(int n) {
    return new InternalResultSet();
  }

  public void reset(CommandContext ctx) {
    executed = false;
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public ResultSet executeInternal(BasicCommandContext ctx) throws CommandExecutionException {
    if (executed) {
      throw new CommandExecutionException("Trying to execute a result-set twice. Please use reset()");
    }
    executed = true;
    ResultSet result = statement.executeDDL(this.ctx);
    if (result instanceof InternalResultSet) {
      ((InternalResultSet) result).plan = this;
    }
    return result;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DDL\n");
    result.append("  ");
    result.append(statement.toString());
    return result.toString();
  }

  @Override
  public Result toResult() {
    ResultInternal result = new ResultInternal();
    result.setProperty("type", "DDLExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("stmText", statement.toString());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    return result;
  }
}
