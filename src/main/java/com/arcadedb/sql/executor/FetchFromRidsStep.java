package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private Collection<PRID> rids;

  private Iterator<PRID> iterator;

  private OResult nextResult = null;

  public FetchFromRidsStep(Collection<PRID> rids, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.rids = rids;
    reset();
  }

  public void reset() {
    iterator = rids.iterator();
    nextResult = null;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      int internalNext = 0;

      private void fetchNext() {
        if (nextResult != null) {
          return;
        }
        while (iterator.hasNext()) {
          PRID nextRid = iterator.next();
          if (nextRid == null) {
            continue;
          }
          PIdentifiable nextDoc = (PIdentifiable) ctx.getDatabase().lookupByRID(nextRid, true);
          if (nextDoc == null) {
            continue;
          }
          nextResult = new OResultInternal();
          ((OResultInternal) nextResult).setElement((PRecord) nextDoc);
          return;
        }
        return;
      }

      @Override
      public boolean hasNext() {
        if (internalNext >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext();
        }
        return nextResult != null;
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }

        internalNext++;
        OResult result = nextResult;
        nextResult = null;
        return result;
      }

      @Override
      public void close() {

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
    return OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM RIDs\n" + OExecutionStepInternal.getIndent(depth, indent)
        + "  " + rids;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (rids != null) {
      result.setProperty("rids", rids.stream().map(x -> x.toString()).collect(Collectors.toList()));
    }
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("rids") != null) {
        List<String> ser = fromResult.getProperty("rids");
        throw new UnsupportedOperationException();
//        rids = ser.stream().map(x -> new PRID(x)).collect(Collectors.toList());
      }
      reset();
    } catch (Exception e) {
      throw new PCommandExecutionException(e);
    }
  }
}
