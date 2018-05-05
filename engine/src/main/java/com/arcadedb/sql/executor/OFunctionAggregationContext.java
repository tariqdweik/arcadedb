package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OFunctionAggregationContext implements AggregationContext {
  private OSQLFunction     aggregateFunction;
  private List<Expression> params;

  public OFunctionAggregationContext(OSQLFunction function, List<Expression> params) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
  }

  @Override public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override public void apply(OResult next, OCommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (Expression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
