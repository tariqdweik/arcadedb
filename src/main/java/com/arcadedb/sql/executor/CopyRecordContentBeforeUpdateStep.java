package com.arcadedb.sql.executor;

import com.arcadedb.database.PBaseRecord;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;

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

  public CopyRecordContentBeforeUpdateStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    OResultSet lastFetched = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = lastFetched.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (result instanceof OUpdatableResult) {
            OResultInternal prevValue = new OResultInternal();
            PRecord rec = result.getElement().get().getRecord();
            prevValue.setProperty("@rid", rec.getIdentity());
            if (rec instanceof PBaseRecord) {
              prevValue.setProperty("@class", ((PBaseRecord) rec).getType());
            }
            for (String propName : result.getPropertyNames()) {
              prevValue.setProperty(propName, result.getProperty(propName));
            }
            ((OUpdatableResult) result).previousValue = prevValue;
          } else {
            throw new PCommandExecutionException("Cannot fetch previous value: " + result);
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
      public Optional<OExecutionPlan> getExecutionPlan() {
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
