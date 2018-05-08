/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.GroupBy;
import com.arcadedb.sql.parser.Projection;
import com.arcadedb.sql.parser.ProjectionItem;

import java.util.*;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final GroupBy groupBy;

  //the key is the GROUP BY key, the value is the (partially) aggregated value
  private Map<List, ResultInternal> aggregateResults = new LinkedHashMap<>();
  private List<ResultInternal>      finalResults     = null;

  private int  nextItem = 0;
  private long cost     = 0;

  public AggregateProjectionCalculationStep(Projection projection, GroupBy groupBy, CommandContext ctx,
      boolean profilingEnabled) {
    super(projection, ctx, profilingEnabled);
    this.groupBy = groupBy;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) {
    if (finalResults == null) {
      executeAggregation(ctx, nRecords);
    }

    return new ResultSet() {
      int localNext = 0;

      @Override
      public boolean hasNext() {
        if (localNext > nRecords || nextItem >= finalResults.size()) {
          return false;
        }
        return true;
      }

      @Override
      public Result next() {
        if (localNext > nRecords || nextItem >= finalResults.size()) {
          throw new IllegalStateException();
        }
        Result result = finalResults.get(nextItem);
        nextItem++;
        localNext++;
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void executeAggregation(CommandContext ctx, int nRecords) {
    if (!prev.isPresent()) {
      throw new CommandExecutionException("Cannot execute an aggregation or a GROUP BY without a previous result");
    }
    ExecutionStepInternal prevStep = prev.get();
    ResultSet lastRs = prevStep.syncPull(ctx, nRecords);
    while (lastRs.hasNext()) {
      aggregate(lastRs.next(), ctx);
      if (!lastRs.hasNext()) {
        lastRs = prevStep.syncPull(ctx, nRecords);
      }
    }
    finalResults = new ArrayList<>();
    finalResults.addAll(aggregateResults.values());
    aggregateResults.clear();
    for (ResultInternal item : finalResults) {
      for (String name : item.getPropertyNames()) {
        Object prevVal = item.getProperty(name);
        if (prevVal instanceof AggregationContext) {
          item.setProperty(name, ((AggregationContext) prevVal).getFinalValue());
        }
      }
    }
  }

  private void aggregate(Result next, CommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      List<Object> key = new ArrayList<>();
      if (groupBy != null) {
        for (Expression item : groupBy.getItems()) {
          Object val = item.execute(next, ctx);
          key.add(val);
        }
      }
      ResultInternal preAggr = aggregateResults.get(key);
      if (preAggr == null) {
        preAggr = new ResultInternal();
        aggregateResults.put(key, preAggr);
      }

      for (ProjectionItem proj : this.projection.getItems()) {
        String alias = proj.getProjectionAlias().getStringValue();
        if (proj.isAggregate()) {
          AggregationContext aggrCtx = preAggr.getProperty(alias);
          if (aggrCtx == null) {
            aggrCtx = proj.getAggregationContext(ctx);
            preAggr.setProperty(alias, aggrCtx);
          }
          aggrCtx.apply(next, ctx);
        } else {
          preAggr.setProperty(alias, proj.execute(next, ctx));
        }
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result +=
        "\n" + spaces + "      " + projection.toString() + "" + (groupBy == null ? "" : (spaces + "\n  " + groupBy.toString()));
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
