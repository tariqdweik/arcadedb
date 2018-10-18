/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;

/**
 * Returns an OResult containing metadata regarding the schema indexes.
 *
 * @author Luca Garulli
 */
public class FetchFromSchemaIndexesStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int  cursor = 0;
  private long cost   = 0;

  public FetchFromSchemaIndexesStep(final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));

    if (cursor == 0) {
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        final Schema schema = ctx.getDatabase().getSchema();

        for (Index index : schema.getIndexes()) {
          final ResultInternal r = new ResultInternal();
          result.add(r);

          r.setProperty("name", index.getName());
          r.setProperty("typeName", index.getTypeName());
          r.setProperty("properties", Arrays.asList(index.getPropertyNames()));
          r.setProperty("unique", index.isUnique());
          r.setProperty("compacting", index.isCompacting());
          r.setProperty("fileId", index.getFileId());
//          r.setProperty("supportsOrderedIterations", index.supportsOrderedIterations());
          r.setProperty("associatedBucketId", index.getAssociatedBucketId());
          try {
            r.setProperty("size", FileUtils.getSizeAsString(ctx.getDatabase().getFileManager().getFile(index.getFileId()).getSize()));
          } catch (IOException e) {
            // IGNORE IT, NO SIZE AVAILABLE
          }
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        result.clear();
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
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA INDEXES";
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
