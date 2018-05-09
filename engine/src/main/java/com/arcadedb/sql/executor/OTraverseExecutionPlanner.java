/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.sql.parser.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTraverseExecutionPlanner {

  private List<TraverseProjectionItem> projections = null;
  private FromClause target;

  private WhereClause whileClause;

  private final TraverseStatement.Strategy strategy;
  private final PInteger                    maxDepth;

  private Skip  skip;
  private Limit limit;

  public OTraverseExecutionPlanner(TraverseStatement statement) {
    //copying the content, so that it can be manipulated and optimized
    this.projections = statement.getProjections() == null ?
        null :
        statement.getProjections().stream().map(x -> x.copy()).collect(Collectors.toList());

    this.target = statement.getTarget();
    this.whileClause = statement.getWhileClause() == null ? null : statement.getWhileClause().copy();

    this.strategy = statement.getStrategy() == null ? TraverseStatement.Strategy.DEPTH_FIRST : statement.getStrategy();
    this.maxDepth = statement.getMaxDepth() == null ? null : statement.getMaxDepth().copy();

    this.skip = statement.getSkip();
    this.limit = statement.getLimit();
  }

  public InternalExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    SelectExecutionPlan result = new SelectExecutionPlan(ctx);

    handleFetchFromTarger(result, ctx, enableProfiling);

    handleTraversal(result, ctx, enableProfiling);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, ctx, enableProfiling));
    }
    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, ctx, enableProfiling));
    }

    return result;
  }

  private void handleTraversal(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    switch (strategy) {
    case BREADTH_FIRST:
      result.chain(new BreadthFirstTraverseStep(this.projections, this.whileClause, maxDepth, ctx, profilingEnabled));
      break;
    case DEPTH_FIRST:
      result.chain(new DepthFirstTraverseStep(this.projections, this.whileClause, maxDepth, ctx, profilingEnabled));
      break;
    }
    //TODO
  }

  private void handleFetchFromTarger(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {

    FromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, this.target, ctx, profilingEnabled);
    } else if (target.getBucket() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getBucket()), ctx, profilingEnabled);
    } else if (target.getBucketList() != null) {
      handleClustersAsTarget(result, target.getBucketList().toListOfClusters(), ctx, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
      throw new CommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(result, target.getInputParam(), ctx, profilingEnabled);
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, target.getIndex(), ctx, profilingEnabled);
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx, profilingEnabled);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx, profilingEnabled);
    } else {
      throw new UnsupportedOperationException();
    }

  }

  private void handleInputParamAsTarget(SelectExecutionPlan result, InputParameter inputParam, CommandContext ctx, boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx, profilingEnabled));//nothing to return
    } else if (paramValue instanceof DocumentType) {
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(((DocumentType) paramValue).getName()));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier((String) paramValue));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof Identifiable) {
      RID orid = ((Identifiable) paramValue).getIdentity();

      Rid rid = new Rid(-1);
      PInteger cluster = new PInteger(-1);
      cluster.setValue(orid.getBucketId());
      PInteger position = new PInteger(-1);
      position.setValue(orid.getPosition());
      rid.setLegacy(true);
      rid.setBucket(cluster);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
    } else if (paramValue instanceof Iterable) {
      //try list of RIDs
      List<Rid> rids = new ArrayList<>();
      for (Object x : (Iterable) paramValue) {
        if (!(x instanceof Identifiable)) {
          throw new CommandExecutionException("Cannot use colleciton as target: " + paramValue);
        }
        RID orid = ((Identifiable) x).getIdentity();

        Rid rid = new Rid(-1);
        PInteger cluster = new PInteger(-1);
        cluster.setValue(orid.getBucketId());
        PInteger position = new PInteger(-1);
        position.setValue(orid.getPosition());
        rid.setBucket(cluster);
        rid.setPosition(position);

        rids.add(rid);
      }
      handleRidsAsTarget(result, rids, ctx, profilingEnabled);
    } else {
      throw new CommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  private void handleIndexAsTarget(SelectExecutionPlan result, IndexIdentifier indexIdentifier, CommandContext ctx, boolean profilingEnabled) {
    String indexName = indexIdentifier.getIndexName();
    Index index = ctx.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:

//      if (!index.supportsOrderedIterations()) {
//        throw new PCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
//      }

      result.chain(new FetchFromIndexStep(index, null, null, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    case VALUES:
    case VALUESASC:
//      if (!index.supportsOrderedIterations()) {
//        throw new PCommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    case VALUESDESC:
//      if (!index.supportsOrderedIterations()) {
//        throw new PCommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    }
  }

  private void handleMetadataAsTarget(SelectExecutionPlan plan, MetadataIdentifier metadata, CommandContext ctx, boolean profilingEnabled) {
    Database db = ctx.getDatabase();
    throw new UnsupportedOperationException();
//    String schemaRecordIdAsString = null;
//    if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_SCHEMA)) {
//      schemaRecordIdAsString = db.getStorage().getConfiguration().getSchemaRecordId();
//    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
//      schemaRecordIdAsString = db.getStorage().getConfiguration().getIndexMgrRecordId();
//    } else {
//      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
//    }
//    ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
//    plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));

  }

  private void handleRidsAsTarget(SelectExecutionPlan plan, List<Rid> rids, CommandContext ctx, boolean profilingEnabled) {
    List<RID> actualRids = new ArrayList<>();
    for (Rid rid : rids) {
      actualRids.add(rid.toRecordId((Result) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled));
  }

  private void handleClassAsTarget(SelectExecutionPlan plan, FromClause queryTarget, CommandContext ctx, boolean profilingEnabled) {
    Identifier identifier = queryTarget.getItem().getIdentifier();

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), null, ctx, orderByRidAsc, profilingEnabled);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(SelectExecutionPlan plan, List<Bucket> clusters, CommandContext ctx, boolean profilingEnabled) {
    Database db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (clusters.size() == 1) {
      Bucket cluster = clusters.get(0);
      java.lang.Integer clusterId = cluster.getBucketNumber();
      if (clusterId == null) {
        clusterId = db.getSchema().getBucketByName(cluster.getBucketName()).getId();
      }
      if (clusterId == null) {
        throw new CommandExecutionException("Cluster " + cluster + " does not exist");
      }
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterId, ctx, profilingEnabled);
      if (Boolean.TRUE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (Boolean.FALSE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      plan.chain(step);
    } else {
      int[] clusterIds = new int[clusters.size()];
      for (int i = 0; i < clusters.size(); i++) {
        Bucket cluster = clusters.get(i);
        java.lang.Integer clusterId = cluster.getBucketNumber();
        if (clusterId == null) {
          clusterId = db.getSchema().getBucketByName(cluster.getBucketName()).getId();
        }
        if (clusterId == null) {
          throw new CommandExecutionException("Cluster " + cluster + " does not exist");
        }
        clusterIds[i] = clusterId;
      }
      FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(clusterIds, ctx, orderByRidAsc, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(SelectExecutionPlan plan, Statement subQuery, CommandContext ctx, boolean profilingEnabled) {
    BasicCommandContext subCtx = new BasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    InternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx, profilingEnabled));
  }

}
