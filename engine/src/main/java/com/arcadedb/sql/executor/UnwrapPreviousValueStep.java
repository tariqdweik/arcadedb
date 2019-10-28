/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  private long cost = 0;

  public UnwrapPreviousValueStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = prev.get().syncPull(ctx, nRecords);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Result prevResult = upstream.next();
          if (prevResult instanceof UpdatableResult) {
            prevResult = ((UpdatableResult) prevResult).previousValue;
            if (prevResult == null) {
              throw new CommandExecutionException("Invalid status of record: no previous value available");
            }
            return prevResult;
          } else {
            throw new CommandExecutionException("Invalid status of record: no previous value available");
          }
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        upstream.close();
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ UNWRAP PREVIOUS VALUE";
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
