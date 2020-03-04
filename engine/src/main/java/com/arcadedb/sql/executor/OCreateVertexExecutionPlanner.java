/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.CreateVertexStatement;
import com.arcadedb.sql.parser.Identifier;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateVertexExecutionPlanner extends OInsertExecutionPlanner {

  public OCreateVertexExecutionPlanner(CreateVertexStatement statement) {
    this.targetType = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetBucketName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.targetBucket = statement.getTargetBucket() == null ? null : statement.getTargetBucket().copy();
    if (this.targetType == null && this.targetBucket == null && this.targetBucketName == null) {
      this.targetType = new Identifier("V");
    }
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
  }

  @Override public InsertExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    InsertExecutionPlan prev = super.createExecutionPlan(ctx, enableProfiling);
    List<ExecutionStep> steps = new ArrayList<>(prev.getSteps());
    InsertExecutionPlan result = new InsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);
    for (ExecutionStep step : steps) {
      result.chain((ExecutionStepInternal) step);
    }
    return result;

  }

  private void handleCheckType(InsertExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    if (targetType != null) {
      result.chain(new CheckClassTypeStep(targetType.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetBucketName != null) {
      result.chain(new CheckClusterTypeStep(targetBucketName.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetBucket != null) {
      result.chain(new CheckClusterTypeStep(targetBucket, "V", ctx, profilingEnabled));
    }
  }
}
