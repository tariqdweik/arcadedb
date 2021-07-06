package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Timeout;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Created by luigidellaquila on 08/08/16. */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {
  private final Timeout timeout;
  private final long timeoutMillis;

  private AtomicLong totalTime = new AtomicLong(0);

  public AccumulatingTimeoutStep(Timeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
    this.timeoutMillis = this.timeout.getVal().longValue();
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws CommandExecutionException {

    final ResultSet internal = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        if (totalTime.get() / 1_000_000 > timeoutMillis) {
          fail();
        }
        long begin = System.nanoTime();

        try {
          return internal.hasNext();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public Result next() {
        if (totalTime.get() / 1_000_000 > timeoutMillis) {
          fail();
        }
        long begin = System.nanoTime();
        try {
          return internal.next();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {
        internal.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return internal.getExecutionPlan();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return internal.getQueryStats();
      }
    };
  }

  private ResultSet fail() {
    this.timedOut = true;
    sendTimeout();
    if (Timeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return new InternalResultSet();
    } else {
      throw new TimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy(), ctx, profilingEnabled);
  }

  @Override
  public void reset() {
    this.totalTime = new AtomicLong(0);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + "ms)";
  }
}
