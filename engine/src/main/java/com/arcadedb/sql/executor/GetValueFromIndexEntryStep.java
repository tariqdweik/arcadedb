/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 16/03/17.
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final int[] filterClusterIds;

  // runtime

  private long cost = 0;

  private ResultSet prevResult = null;

  /**
   * @param ctx              the execution context
   * @param filterClusterIds only extract values from these clusters. Pass null if no filtering is needed
   * @param profilingEnabled enable profiling
   */
  public GetValueFromIndexEntryStep(CommandContext ctx, int[] filterClusterIds, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.filterClusterIds = filterClusterIds;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {

    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    ExecutionStepInternal prevStep = prev.get();

    return new ResultSet() {

      public boolean finished = false;

      Result nextItem = null;
      int fetched = 0;

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        return nextItem != null;

      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished) {
          throw new IllegalStateException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new IllegalStateException();
        }
        Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

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
          Result val = prevResult.next();
          long begin = profilingEnabled ? System.nanoTime() : 0;

          try {
            Object finalVal = val.getProperty("rid");
            if (filterClusterIds != null) {
              if (!(finalVal instanceof Identifiable)) {
                continue;
              }
              RID rid = ((Identifiable) finalVal).getIdentity();
              boolean found = false;
              for (int filterClusterId : filterClusterIds) {
                if (rid.getBucketId() < 0 || filterClusterId == rid.getBucketId()) {
                  found = true;
                  break;
                }
              }
              if (!found) {
                continue;
              }
            }

            if (finalVal instanceof RID) {
              final ResultInternal res = new ResultInternal();
              res.setElement(((RID) finalVal).getDocument());
              nextItem = res;
            } else if (finalVal instanceof Document) {
              final ResultInternal res = new ResultInternal();
              res.setElement((Document) finalVal);
              nextItem = res;
            } else if (finalVal instanceof Result) {
              nextItem = (Result) finalVal;
            }
            break;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
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
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXTRACT VALUE FROM INDEX ENTRY";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (filterClusterIds != null) {
      result += "\n";
      result += spaces;
      result += "  filtering clusters [";
      result += Arrays.stream(filterClusterIds).boxed().map(x -> "" + x).collect(Collectors.joining(","));
      result += "]";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new GetValueFromIndexEntryStep(ctx, this.filterClusterIds, this.profilingEnabled);
  }
}
