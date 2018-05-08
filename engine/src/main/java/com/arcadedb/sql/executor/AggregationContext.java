package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 16/07/16.
 */
public interface AggregationContext {


  public Object getFinalValue();

  void apply(Result next, CommandContext ctx);
}
