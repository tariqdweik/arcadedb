/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class FunctionAggregationContext implements AggregationContext {
  private SQLFunction      aggregateFunction;
  private List<Expression> params;

  public FunctionAggregationContext(SQLFunction function, List<Expression> params) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
  }

  @Override public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override public void apply(Result next, CommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (Expression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
