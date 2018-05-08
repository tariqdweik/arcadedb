/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromDatabaseMetadataStep extends AbstractExecutionStep {

  boolean served = false;
  long    cost   = 0;

  public FetchFromDatabaseMetadataStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !served;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        throw new UnsupportedOperationException();
//        try {
//
//          if (!served) {
//            OResultInternal result = new OResultInternal();
//
//            ODatabase db = ctx.getDatabase();
//            result.setProperty("name", db.getName());
//            result.setProperty("user", db.getUser() == null ? null : db.getUser().getName());
//            result.setProperty("type", String.valueOf(db.get(ODatabase.ATTRIBUTES.TYPE)));
//            result.setProperty("status", String.valueOf(db.get(ODatabase.ATTRIBUTES.STATUS)));
//            result.setProperty("defaultClusterId", String.valueOf(db.get(ODatabase.ATTRIBUTES.DEFAULTCLUSTERID)));
//            result.setProperty("dateFormat", String.valueOf(db.get(ODatabase.ATTRIBUTES.DATEFORMAT)));
//            result.setProperty("dateTimeFormat", String.valueOf(db.get(ODatabase.ATTRIBUTES.DATETIMEFORMAT)));
//            result.setProperty("timezone", String.valueOf(db.get(ODatabase.ATTRIBUTES.TIMEZONE)));
//            result.setProperty("localeCountry", String.valueOf(db.get(ODatabase.ATTRIBUTES.LOCALECOUNTRY)));
//            result.setProperty("localeLanguage", String.valueOf(db.get(ODatabase.ATTRIBUTES.LOCALELANGUAGE)));
//            result.setProperty("charset", String.valueOf(db.get(ODatabase.ATTRIBUTES.CHARSET)));
//            result.setProperty("clusterSelection", String.valueOf(db.get(ODatabase.ATTRIBUTES.CLUSTERSELECTION)));
//            result.setProperty("minimumClusters", String.valueOf(db.get(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS)));
//            result.setProperty("conflictStrategy", String.valueOf(db.get(ODatabase.ATTRIBUTES.CONFLICTSTRATEGY)));
//            result.setProperty("validation", String.valueOf(db.get(ODatabase.ATTRIBUTES.VALIDATION)));
//
//            served = true;
//            return result;
//          }
//          throw new IllegalStateException();
//        } finally {
//          if (profilingEnabled) {
//            cost += (System.nanoTime() - begin);
//          }
//        }
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
