package com.arcadedb.sql.parser;

import com.arcadedb.database.PDatabase;
import com.arcadedb.sql.executor.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by luigidellaquila on 12/08/16.
 */
public abstract class ODDLStatement extends Statement {

  public ODDLStatement(int id) {
    super(id);
  }

  public ODDLStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OResultSet executeDDL(OCommandContext ctx);

  public OResultSet execute(PDatabase db, Object[] args, OCommandContext parentCtx) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OResultSet execute(PDatabase db, Map params, OCommandContext parentCtx) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new ODDLExecutionPlan(ctx, this);
  }

}
