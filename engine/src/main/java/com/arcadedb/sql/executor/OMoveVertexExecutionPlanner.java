/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.parser.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OMoveVertexExecutionPlanner {
  private final FromItem         source;
  private final Identifier       targetClass;
  private final Bucket           targetCluster;
  private final UpdateOperations updateOperations;
  private final Batch            batch;

  public OMoveVertexExecutionPlanner(MoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetClass = oStatement.getTargetType();
    this.targetCluster = oStatement.getTargetBucket();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public UpdateExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    UpdateExecutionPlan result = new UpdateExecutionPlan(ctx);

    handleSource(result, ctx, this.source, enableProfiling);
    convertToModifiableResult(result, ctx, enableProfiling);
    handleTarget(result, targetClass, targetCluster, ctx, enableProfiling);
    handleOperations(result, ctx, this.updateOperations, enableProfiling);
    handleBatch(result, ctx, this.batch, enableProfiling);
    handleSave(result, ctx, enableProfiling);
    return result;
  }

  private void handleTarget(UpdateExecutionPlan result, Identifier targetClass, Bucket targetCluster, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new MoveVertexStep(targetClass, targetCluster, ctx, profilingEnabled));
  }

  private void handleBatch(UpdateExecutionPlan result, CommandContext ctx, Batch batch, boolean profilingEnabled) {
    if (batch != null) {
      result.chain(new BatchStep(batch, ctx, profilingEnabled));
    }
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty() updates the actual PIdentifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(UpdateExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new ConvertToUpdatableResultStep(ctx, profilingEnabled));
  }

  private void handleResultForReturnCount(UpdateExecutionPlan result, CommandContext ctx, boolean returnCount, boolean profilingEnabled) {
    if (returnCount) {
      result.chain(new CountStep(ctx, profilingEnabled));
    }
  }

  private void handleResultForReturnAfter(UpdateExecutionPlan result, CommandContext ctx, boolean returnAfter,
      Projection returnProjection, boolean profilingEnabled) {
    if (returnAfter) {
      //re-convert to normal step
      result.chain(new ConvertToResultInternalStep(ctx, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx, profilingEnabled));
      }
    }
  }

  private void handleResultForReturnBefore(UpdateExecutionPlan result, CommandContext ctx, boolean returnBefore,
      Projection returnProjection, boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep(ctx, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx, profilingEnabled));
      }
    }
  }

  private void handleSave(UpdateExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, profilingEnabled));
  }

  private void handleTimeout(UpdateExecutionPlan result, CommandContext ctx, Timeout timeout, boolean profilingEnabled) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout, ctx, profilingEnabled));
    }
  }

  private void handleReturnBefore(UpdateExecutionPlan result, CommandContext ctx, boolean returnBefore, boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new CopyRecordContentBeforeUpdateStep(ctx, profilingEnabled));
    }
  }

  private void handleLock(UpdateExecutionPlan result, CommandContext ctx, Object lockRecord) {

  }

  private void handleLimit(UpdateExecutionPlan plan, CommandContext ctx, Limit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleUpsert(UpdateExecutionPlan plan, CommandContext ctx, FromClause target, WhereClause where,
      boolean upsert, boolean profilingEnabled) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where, ctx, profilingEnabled));
    }
  }

  private void handleOperations(UpdateExecutionPlan plan, CommandContext ctx, UpdateOperations op, boolean profilingEnabled) {
    if (op != null) {
      switch (op.getType()) {
      case UpdateOperations.TYPE_SET:
        plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx, profilingEnabled));
        break;
      case UpdateOperations.TYPE_REMOVE:
        plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx, profilingEnabled));
        break;
      case UpdateOperations.TYPE_MERGE:
        plan.chain(new UpdateMergeStep(op.getJson(), ctx, profilingEnabled));
        break;
      case UpdateOperations.TYPE_CONTENT:
        plan.chain(new UpdateContentStep(op.getJson(), ctx, profilingEnabled));
        break;
      case UpdateOperations.TYPE_PUT:
      case UpdateOperations.TYPE_INCREMENT:
      case UpdateOperations.TYPE_ADD:
        throw new CommandExecutionException("Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(UpdateExecutionPlan result, CommandContext ctx, FromItem source, boolean profilingEnabled) {
    SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(new FromClause(-1));
    sourceStatement.getTarget().setItem(source);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }
}