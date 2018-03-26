package com.arcadedb.sql.parser;

import com.arcadedb.database.PDatabase;
import com.arcadedb.sql.executor.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner.
 * All the execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class OSimpleExecStatement extends OStatement {

  public OSimpleExecStatement(int id) {
    super(id);
  }

  public OSimpleExecStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OResultSet executeSimple(OCommandContext ctx);

  public OResultSet execute(PDatabase db, Object[] args, OCommandContext parentContext) {
    OBasicCommandContext ctx = new OBasicCommandContext();
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
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OResultSet execute(PDatabase db, Map params, OCommandContext parentContext) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new OSingleOpExecutionPlan(ctx, this);
  }

}
