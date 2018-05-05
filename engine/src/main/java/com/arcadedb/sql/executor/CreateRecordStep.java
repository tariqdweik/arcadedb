package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PVertexType;

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

  public CreateRecordStep(final String typeName, OCommandContext ctx, int total, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.typeName = typeName;
    this.total = total;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      int locallyCreated = 0;

      @Override
      public boolean hasNext() {
        if (locallyCreated >= nRecords) {
          return false;
        }
        return created < total;
      }

      @Override
      public OResult next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          created++;
          locallyCreated++;

          final PDocumentType type = ctx.getDatabase().getSchema().getType(typeName);

          final PDocument instance;
          if (type instanceof PVertexType)
            instance = ctx.getDatabase().newVertex(typeName);
          else if (type instanceof PEdgeType)
            throw new IllegalArgumentException("Cannot instantiate an edge");
          else
            instance = ctx.getDatabase().newDocument(typeName);

          return new OUpdatableResult((PModifiableDocument) instance);
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
