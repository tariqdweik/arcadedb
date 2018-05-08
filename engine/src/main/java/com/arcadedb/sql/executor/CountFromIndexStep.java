package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.IndexIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Returns the number of records contained in an index
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromIndexStep extends AbstractExecutionStep {
  private final IndexIdentifier target;
  private final String          alias;

  private long count = 0;

  private boolean executed = false;

  /**
   * @param targetIndex the index name as it is parsed by the SQL parsed
   * @param alias the name of the property returned in the result-set
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromIndexStep(IndexIdentifier targetIndex, String alias, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetIndex;
    this.alias = alias;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          //TODO
          throw new UnsupportedOperationException("TODO");
//          OIndex<?> idx = ctx.getDatabase().getMetadata().getIndexManager().getIndex(target.getIndexName());
//          long size = idx.getSize();
//          executed = true;
//          OResultInternal result = new OResultInternal();
//          result.setProperty(alias, size);
//          return result;
        } finally {
          count += (System.nanoTime() - begin);
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
        CountFromIndexStep.this.reset();
      }
    };
  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE: " + target;
  }
}
