package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws PCommandExecutionException in case
 * it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {
  private final String clazz;

  private long cost = 0;

  public CheckRecordTypeStep(CommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clazz = className;
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
        Result result = upstream.next();
        throw new UnsupportedOperationException("TODO");
//        long begin = profilingEnabled ? System.nanoTime() : 0;
//        try {
//          if (!result.isElement()) {
//            throw new PCommandExecutionException("record " + result + " is not an instance of " + clazz);
//          }
//          PRecord doc = result.getElement().get();
//          if (doc == null) {
//            throw new PCommandExecutionException("record " + result + " is not an instance of " + clazz);
//          }
//          Optional<PType> schema = doc.getSchemaType();
//
//          if (!schema.isPresent() || !schema.get().isSubClassOf(clazz)) {
//            throw new PCommandExecutionException("record " + result + " is not an instance of " + clazz);
//          }
//          return result;
//        } finally {
//          if (profilingEnabled) {
//            cost += (System.nanoTime() - begin);
//          }
//        }
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
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CHECK RECORD TYPE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (ExecutionStepInternal.getIndent(depth, indent) + "  " + clazz);
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
