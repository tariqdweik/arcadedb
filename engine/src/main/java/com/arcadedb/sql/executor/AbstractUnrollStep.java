/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {


  ResultSet        lastResult      = null;
  Iterator<Result> nextSubsequence = null;
  Result           nextElement     = null;

  public AbstractUnrollStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override public void reset() {
    this.lastResult = null;
    this.nextSubsequence = null;
    this.nextElement = null;
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) {
    if (prev == null || !prev.isPresent()) {
      throw new CommandExecutionException("Cannot expand without a target");
    }
    return new ResultSet() {
      long localCount = 0;

      @Override public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        return nextElement != null;
      }

      @Override public Result next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextElement == null) {
          throw new IllegalStateException();
        }

        Result result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(ctx, nRecords);
        return result;
      }

      @Override public void close() {

      }

      @Override public Optional<ExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNext(CommandContext ctx, int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().get().syncPull(ctx, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      Result nextAggregateItem = lastResult.next();
      nextSubsequence = unroll(nextAggregateItem, ctx).iterator();

    } while (true);

  }

  protected abstract Collection<Result> unroll(final Result doc, final CommandContext iContext);

}
