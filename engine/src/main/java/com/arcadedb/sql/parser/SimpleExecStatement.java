/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.sql.executor.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner.
 * All the execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class SimpleExecStatement extends Statement {

  public SimpleExecStatement(int id) {
    super(id);
  }

  public SimpleExecStatement(SqlParser p, int id) {
    super(p, id);
  }

  public abstract ResultSet executeSimple(CommandContext ctx);

  public ResultSet execute(Database db, Object[] args, CommandContext parentContext) {
    BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public ResultSet execute(Database db, Map params, CommandContext parentContext) {
    BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public InternalExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    return new SingleOpExecutionPlan(ctx, this);
  }

}
