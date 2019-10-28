/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Bucket;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 14/02/17.
 */
public class MoveVertexStep extends AbstractExecutionStep {
  private String targetCluster;
  private String targetClass;

  public MoveVertexStep(Identifier targetClass, Bucket targetCluster, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass == null ? null : targetClass.getStringValue();
    if (targetCluster != null) {
      this.targetCluster = targetCluster.getBucketName();
      if (this.targetCluster == null) {
        this.targetCluster = ctx.getDatabase().getSchema().getBucketById(targetCluster.getBucketNumber()).getName();
      }
    }
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result current = upstream.next();
        throw new UnsupportedOperationException();
//        current.getVertexRID().ifPresent(x -> x.moveTo(targetClass, targetCluster));
//        return current;
      }

      @Override
      public void close() {
        upstream.close();
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
    result.append("+ MOVE VERTEX TO ");
    if (targetClass != null) {
      result.append("USERTYPE ");
      result.append(targetClass);
    }
    if (targetCluster != null) {
      result.append("CLUSTER ");
      result.append(targetCluster);
    }
    return result.toString();
  }
}
