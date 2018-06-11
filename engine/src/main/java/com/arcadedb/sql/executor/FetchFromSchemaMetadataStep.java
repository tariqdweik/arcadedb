/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Returns an OResult containing metadata regarding the schema.
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromSchemaMetadataStep extends AbstractExecutionStep {

  boolean served = false;
  long    cost   = 0;

  public FetchFromSchemaMetadataStep(CommandContext ctx, boolean profilingEnabled) {
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
        try {

          if (!served) {
            final ResultInternal result = new ResultInternal();

            final Schema schema = ctx.getDatabase().getSchema();

            final List<ResultInternal> types = new ArrayList<>();

            for (DocumentType type : schema.getTypes()) {
              final ResultInternal r = new ResultInternal();
              types.add(r);

              r.setProperty("name", type.getName());
//              r.setProperty("buckets", type.getBuckets(false));
//              r.setProperty("properties", type.getPropertyNames());
            }

            result.setProperty("types", types);

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
