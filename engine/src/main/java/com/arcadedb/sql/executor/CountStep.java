/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

/**
 * Counts the records from the previous steps.
 * Returns a record with a single property, called "count" containing the count of records received from pervious steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CountStep extends AbstractExecutionStep {

  private long cost = 0;

  boolean executed = false;

  /**
   *
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (executed) {
      return new InternalResultSet();
    }
    ResultInternal resultRecord = new ResultInternal();
    executed = true;
    long count = 0;
    while (true) {
      ResultSet prevResult = getPrev().get().syncPull(ctx, nRecords);

      if (!prevResult.hasNext()) {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          InternalResultSet result = new InternalResultSet();
          resultRecord.setProperty("count", count);
          result.add(resultRecord);
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }
      while (prevResult.hasNext()) {
        count++;
        prevResult.next();
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
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
