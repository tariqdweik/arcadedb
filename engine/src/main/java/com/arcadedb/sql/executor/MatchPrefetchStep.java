/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchPrefetchStep extends AbstractExecutionStep {

  public static final String PREFETCHED_MATCH_ALIAS_PREFIX = "__$$OrientDB_Prefetched_Alias_Prefix__";

  private final String                alias;
  private final InternalExecutionPlan prefetchExecutionPlan;

  boolean executed = false;

  public MatchPrefetchStep(CommandContext ctx, InternalExecutionPlan prefetchExecPlan, String alias, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.prefetchExecutionPlan = prefetchExecPlan;
    this.alias = alias;
  }

  @Override
  public void reset() {
    executed = false;
    prefetchExecutionPlan.reset(ctx);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (!executed) {
      getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));

      ResultSet nextBlock = prefetchExecutionPlan.fetchNext(nRecords);
      List<Result> prefetched = new ArrayList<>();
      while (nextBlock.hasNext()) {
        while (nextBlock.hasNext()) {
          prefetched.add(nextBlock.next());
        }
        nextBlock = prefetchExecutionPlan.fetchNext(nRecords);
      }
      prefetchExecutionPlan.close();
      ctx.setVariable(PREFETCHED_MATCH_ALIAS_PREFIX + alias, prefetched);
      executed = true;
    }
    return new InternalResultSet();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ PREFETCH " + alias + "\n");
    result.append(prefetchExecutionPlan.prettyPrint(depth + 1, indent));
    return result.toString();
  }
}
