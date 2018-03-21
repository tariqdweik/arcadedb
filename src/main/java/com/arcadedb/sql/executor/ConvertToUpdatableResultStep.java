package com.arcadedb.sql.executor;

import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PTimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * <p>
 * takes a normal result set and transforms it in another result set made of OUpdatableRecord instances.
 * Records that are not identifiable are discarded.
 * </p>
 * <p>This is the opposite of ConvertToResultInternalStep</p>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ConvertToUpdatableResultStep extends AbstractExecutionStep {

  private long cost = 0;

  OResultSet prevResult = null;

  public ConvertToUpdatableResultStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OResultSet() {
      public boolean finished = false;

      OResult nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(ctx, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(ctx, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nextItem instanceof OUpdatableResult) {
              break;
            }
            if (nextItem.isElement()) {
              PRecord element = nextItem.getElement().get();
              if (element != null && element instanceof PModifiableDocument) {
                nextItem = new OUpdatableResult((PModifiableDocument) element);
              }
              break;
            }

            nextItem = null;
          } finally {
            cost = (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        if (nextItem != null) {
          return true;
        }

        return false;
      }

      @Override
      public OResult next() {
        if (fetched >= nRecords || finished) {
          throw new IllegalStateException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new IllegalStateException();
        }
        OResult result = nextItem;
        nextItem = null;
        fetched++;
        ctx.setVariable("$current", result);
        return result;
      }

      @Override
      public void close() {
        ConvertToUpdatableResultStep.this.close();
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
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CONVERT TO UPDATABLE ITEM";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}

