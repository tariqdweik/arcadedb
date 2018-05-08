package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

public class LockRecordStep extends AbstractExecutionStep {
  //  private final OStorage.LOCKING_STRATEGY lockStrategy;
  private final Object lockStrategy;

  public LockRecordStep(Object lockStrategy, CommandContext ctx, boolean enableProfiling) {
//    public LockRecordStep(OStorage.LOCKING_STRATEGY lockStrategy, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.lockStrategy = lockStrategy;
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
//        result.getElement().ifPresent(x -> ctx.getDatabase().getTransaction().lockRecord(x, lockStrategy));
        return result;
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ LOCK RECORD");
    result.append("\n");
    result.append(spaces);
    result.append("  lock strategy: " + lockStrategy);

    return result.toString();
  }
}
