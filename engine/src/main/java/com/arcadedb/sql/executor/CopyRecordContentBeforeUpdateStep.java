package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * <p>Reads an upstream result set and returns a new result set that contains copies of the original OResult instances
 * </p>
 * <p>This is mainly used from statements that need to copy of the original data before modifying it,
 * eg. UPDATE ... RETURN BEFORE</p>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CopyRecordContentBeforeUpdateStep extends AbstractExecutionStep {
  private long cost = 0;

  public CopyRecordContentBeforeUpdateStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet lastFetched = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override
      public Result next() {
        Result result = lastFetched.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (result instanceof UpdatableResult) {
            ResultInternal prevValue = new ResultInternal();
            Record rec = result.getElement().get().getRecord();
            prevValue.setProperty("@rid", rec.getIdentity());
            if (rec instanceof Document) {
              prevValue.setProperty("@class", ((Document) rec).getType());
            }
            for (String propName : result.getPropertyNames()) {
              prevValue.setProperty(propName, result.getProperty(propName));
            }
            ((UpdatableResult) result).previousValue = prevValue;
          } else {
            throw new CommandExecutionException("Cannot fetch previous value: " + result);
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
        lastFetched.close();
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
    result.append("+ COPY RECORD CONTENT BEFORE UPDATE");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }

}
