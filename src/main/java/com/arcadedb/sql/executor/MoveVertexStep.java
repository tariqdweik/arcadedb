package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.Cluster;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class MoveVertexStep extends AbstractExecutionStep {
  private String targetCluster;
  private String targetClass;

  public MoveVertexStep(Identifier targetClass, Cluster targetCluster, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass == null ? null : targetClass.getStringValue();
    if (targetCluster != null) {
      this.targetCluster = targetCluster.getClusterName();
      if (this.targetCluster == null) {
        this.targetCluster = ctx.getDatabase().getSchema().getBucketById(targetCluster.getClusterNumber()).getName();
      }
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult current = upstream.next();
        throw new UnsupportedOperationException();
//        current.getVertex().ifPresent(x -> x.moveTo(targetClass, targetCluster));
//        return current;
      }

      @Override
      public void close() {
        upstream.close();
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
    result.append("+ MOVE VERTEX TO ");
    if (targetClass != null) {
      result.append("CLASS ");
      result.append(targetClass);
    }
    if (targetCluster != null) {
      result.append("CLUSTER ");
      result.append(targetCluster);
    }
    return result.toString();
  }
}
