/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromSchemaDatabaseStep extends AbstractExecutionStep {

  boolean served = false;
  long    cost   = 0;

  public FetchFromSchemaDatabaseStep(final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !served;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (!served) {
            final ResultInternal result = new ResultInternal();

            final Database db = ctx.getDatabase();
            result.setProperty("name", db.getName());
            result.setProperty("path", db.getDatabasePath());
            result.setProperty("mode", db.getMode());
            result.setProperty("settings", db.getConfiguration().getContextKeys());
            result.setProperty("dateFormat", db.getSchema().getDateFormat());
            result.setProperty("dateTimeFormat", db.getSchema().getDateTimeFormat());
            result.setProperty("timezone", db.getSchema().getTimeZone().getDisplayName());
            result.setProperty("encoding", db.getSchema().getEncoding());

            served = true;
            return result;
          }
          throw new IllegalStateException();
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
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

      @Override
      public void reset() {
        served = false;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
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
