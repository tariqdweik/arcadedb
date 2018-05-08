package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.OrderBy;

import java.util.*;

/**
 * Created by luigidellaquila on 11/07/16.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderBy orderBy;
  private       Integer maxResults;

  private long cost = 0;

  List<Result> cachedResult = null;
  int          nextElement  = 0;

  public OrderByStep(OrderBy orderBy, CommandContext ctx, boolean profilingEnabled) {
    this(orderBy, null, ctx, profilingEnabled);
  }

  public OrderByStep(OrderBy orderBy, Integer maxResults, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (cachedResult == null) {
      cachedResult = new ArrayList<>();
      prev.ifPresent(p -> init(p, ctx));
    }

    return new ResultSet() {
      int currentBatchReturned = 0;
      int offset = nextElement;

      @Override
      public boolean hasNext() {
        if (currentBatchReturned >= nRecords) {
          return false;
        }
        if (cachedResult.size() <= nextElement) {
          return false;
        }
        return true;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (currentBatchReturned >= nRecords) {
            throw new IllegalStateException();
          }
          if (cachedResult.size() <= nextElement) {
            throw new IllegalStateException();
          }
          Result result = cachedResult.get(offset + currentBatchReturned);
          nextElement++;
          currentBatchReturned++;
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        prev.ifPresent(p -> p.close());
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return new HashMap<>();
      }
    };
  }

  private void init(ExecutionStepInternal p, CommandContext ctx) {

    boolean sorted = true;
    do {
      ResultSet lastBatch = p.syncPull(ctx, 100);
      if (!lastBatch.hasNext()) {
        break;
      }
      while (lastBatch.hasNext()) {
        if (this.timedOut) {
          break;
        }
        Result item = lastBatch.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          cachedResult.add(item);
          sorted = false;
          //compact, only at twice as the buffer, to avoid to do it at each add
          if (this.maxResults != null && maxResults * 2 < cachedResult.size()) {
            cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
            cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
            sorted = true;
          }
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }
      if (timedOut) {
        break;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        //compact at each batch, if needed
        if (!sorted && this.maxResults != null && maxResults < cachedResult.size()) {
          cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
          cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
          sorted = true;
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    } while (true);
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (!sorted) {
        cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (maxResults != null ? "\n  (buffer size: " + maxResults + ")" : "");
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
