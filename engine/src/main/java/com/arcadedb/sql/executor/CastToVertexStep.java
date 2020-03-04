/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 20/02/17.
 */
public class CastToVertexStep extends AbstractExecutionStep {

  private long cost;

  public CastToVertexStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result result = upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (result.getElement().orElse(null) instanceof Vertex) {
            return result;
          }
          if (result.isVertex()) {
            if (result instanceof ResultInternal) {
              ((ResultInternal) result).setElement(result.getElement().get());
            } else {
              ResultInternal r = new ResultInternal();
              r.setElement(result.getElement().get());
              result = r;
            }
          } else {
            throw new CommandExecutionException("Current element is not a vertex: " + result);
          }
          return result;
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
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO VERTEX";
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
