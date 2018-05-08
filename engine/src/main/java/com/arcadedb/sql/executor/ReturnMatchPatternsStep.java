package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 12/10/16.
 */
public class ReturnMatchPatternsStep extends AbstractExecutionStep {

  public ReturnMatchPatternsStep(CommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
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
        return filter(upstream.next());
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

  private Result filter(Result next) {
    next.getPropertyNames().stream().filter(s -> s.startsWith(OMatchExecutionPlanner.DEFAULT_ALIAS_PREFIX))
        .forEach(((ResultInternal) next)::removeProperty);
    return next;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ RETURN $patterns";
  }
}
