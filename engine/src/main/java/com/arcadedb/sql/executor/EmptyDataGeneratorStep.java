/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private long cost = 0;

  int size;
  int served = 0;

  public EmptyDataGeneratorStep(int size, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.size = size;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return served < size;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (served < size) {
            served++;
            ResultInternal result = new ResultInternal();
            ctx.setVariable("$current", result);
            return result;
          }
          throw new IllegalStateException();
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
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

      @Override
      public void reset() {
        served = 0;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
