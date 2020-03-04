/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreateRecordStep extends AbstractExecutionStep {

  private long cost = 0;

  int created = 0;
  int total   = 0;

  String typeName = null;

  public CreateRecordStep(final String typeName, CommandContext ctx, int total, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.typeName = typeName;
    this.total = total;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      int locallyCreated = 0;

      @Override
      public boolean hasNext() {
        if (locallyCreated >= nRecords) {
          return false;
        }
        return created < total;
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          created++;
          locallyCreated++;

          final DocumentType type = ctx.getDatabase().getSchema().getType(typeName);

          final Document instance;
          if (type instanceof VertexType)
            instance = ctx.getDatabase().newVertex(typeName);
          else if (type instanceof EdgeType)
            throw new IllegalArgumentException("Cannot instantiate an edge");
          else
            instance = ctx.getDatabase().newDocument(typeName);

          return new UpdatableResult((MutableDocument) instance);
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
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CREATE EMPTY RECORDS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append(spaces);
    if (total == 1) {
      result.append("  1 record");
    } else {
      result.append("  " + total + " record");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
