package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OInsertExecutionPlanner {

  protected Identifier      targetClass;
  protected Identifier      targetClusterName;
  protected Cluster         targetCluster;
  protected IndexIdentifier targetIndex;
  protected InsertBody      insertBody;
  protected Projection      returnStatement;
  protected SelectStatement selectStatement;

  public OInsertExecutionPlanner() {

  }

  public OInsertExecutionPlanner(InsertStatement statement) {
    this.targetClass = statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName = statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster = statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    this.targetIndex = statement.getTargetIndex() == null ? null : statement.getTargetIndex().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement = statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    if (targetIndex != null) {
      result.chain(new InsertIntoIndexStep(targetIndex, insertBody, ctx, enableProfiling));
    } else {
      if (selectStatement != null) {
        handleInsertSelect(result, this.selectStatement, ctx, enableProfiling);
      } else {
        handleCreateRecord(result, this.insertBody, ctx, enableProfiling);
      }
      handleTargetClass(result, targetClass, ctx, enableProfiling);
      handleSetFields(result, insertBody, ctx, enableProfiling);
      handleReturn(result, returnStatement, ctx, enableProfiling);
      if (targetCluster != null) {
        String name = targetCluster.getClusterName();
        if (name == null) {
          name = ctx.getDatabase().getSchema().getBucketById(targetCluster.getClusterNumber()).getName();
        }
        handleSave(result, new Identifier(name), ctx, enableProfiling);
      } else {
        handleSave(result, targetClusterName, ctx, enableProfiling);
      }
    }
    return result;
  }

  private void handleSave(OInsertExecutionPlan result, Identifier targetClusterName, OCommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleReturn(OInsertExecutionPlan result, Projection returnStatement, OCommandContext ctx,
      boolean profilingEnabled) {
    if (returnStatement != null) {
      result.chain(new ProjectionCalculationStep(returnStatement, ctx, profilingEnabled));
    }
  }

  private void handleSetFields(OInsertExecutionPlan result, InsertBody insertBody, OCommandContext ctx, boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), ctx, profilingEnabled));
    } else if (insertBody.getContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getContent(), ctx, profilingEnabled));
    } else if (insertBody.getSetExpressions() != null) {
      List<UpdateItem> items = new ArrayList<>();
      for (InsertSetExpression exp : insertBody.getSetExpressions()) {
        UpdateItem item = new UpdateItem(-1);
        item.setOperator(UpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(OInsertExecutionPlan result, Identifier targetClass, OCommandContext ctx,
      boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(new SetDocumentClassStep(targetClass, ctx, profilingEnabled));
    }
  }

  private void handleCreateRecord(OInsertExecutionPlan result, InsertBody body, OCommandContext ctx, boolean profilingEnabled) {
    int tot = 1;

    if (body != null && body.getValueExpressions() != null && body.getValueExpressions().size() > 0) {
      tot = body.getValueExpressions().size();
    }
    result.chain(new CreateRecordStep(ctx, tot, profilingEnabled));
  }

  private void handleInsertSelect(OInsertExecutionPlan result, SelectStatement selectStatement, OCommandContext ctx,
      boolean profilingEnabled) {
    OInternalExecutionPlan subPlan = selectStatement.createExecutionPlan(ctx, profilingEnabled);
    result.chain(new SubQueryStep(subPlan, ctx, ctx, profilingEnabled));
    result.chain(new CopyDocumentStep(ctx, profilingEnabled));
    result.chain(new RemoveEdgePointersStep(ctx, profilingEnabled));
  }

}