package com.arcadedb.sql.executor;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRID;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.index.PIndex;
import com.arcadedb.schema.PDocumentType;
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

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);

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

  private void handleTraversal(OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
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

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {

    FromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, this.target, ctx, profilingEnabled);
    } else if (target.getCluster() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getCluster()), ctx, profilingEnabled);
    } else if (target.getClusterList() != null) {
      handleClustersAsTarget(result, target.getClusterList().toListOfClusters(), ctx, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
      throw new PCommandExecutionException("function call as target is not supported yet");
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

  private void handleInputParamAsTarget(OSelectExecutionPlan result, InputParameter inputParam, OCommandContext ctx, boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx, profilingEnabled));//nothing to return
    } else if (paramValue instanceof PDocumentType) {
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(((PDocumentType) paramValue).getName()));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier((String) paramValue));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof PIdentifiable) {
      PRID orid = ((PIdentifiable) paramValue).getIdentity();

      Rid rid = new Rid(-1);
      PInteger cluster = new PInteger(-1);
      cluster.setValue(orid.getBucketId());
      PInteger position = new PInteger(-1);
      position.setValue(orid.getPosition());
      rid.setLegacy(true);
      rid.setCluster(cluster);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
    } else if (paramValue instanceof Iterable) {
      //try list of RIDs
      List<Rid> rids = new ArrayList<>();
      for (Object x : (Iterable) paramValue) {
        if (!(x instanceof PIdentifiable)) {
          throw new PCommandExecutionException("Cannot use colleciton as target: " + paramValue);
        }
        PRID orid = ((PIdentifiable) x).getIdentity();

        Rid rid = new Rid(-1);
        PInteger cluster = new PInteger(-1);
        cluster.setValue(orid.getBucketId());
        PInteger position = new PInteger(-1);
        position.setValue(orid.getPosition());
        rid.setCluster(cluster);
        rid.setPosition(position);

        rids.add(rid);
      }
      handleRidsAsTarget(result, rids, ctx, profilingEnabled);
    } else {
      throw new PCommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  private void handleIndexAsTarget(OSelectExecutionPlan result, IndexIdentifier indexIdentifier, OCommandContext ctx, boolean profilingEnabled) {
    String indexName = indexIdentifier.getIndexName();
    PIndex index = ctx.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new PCommandExecutionException("Index not found: " + indexName);
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

  private void handleMetadataAsTarget(OSelectExecutionPlan plan, MetadataIdentifier metadata, OCommandContext ctx, boolean profilingEnabled) {
    PDatabase db = ctx.getDatabase();
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

  private void handleRidsAsTarget(OSelectExecutionPlan plan, List<Rid> rids, OCommandContext ctx, boolean profilingEnabled) {
    List<PRID> actualRids = new ArrayList<>();
    for (Rid rid : rids) {
      actualRids.add(rid.toRecordId((OResult) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled));
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, FromClause queryTarget, OCommandContext ctx, boolean profilingEnabled) {
    Identifier identifier = queryTarget.getItem().getIdentifier();

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), null, ctx, orderByRidAsc, profilingEnabled);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(OSelectExecutionPlan plan, List<Cluster> clusters, OCommandContext ctx, boolean profilingEnabled) {
    PDatabase db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (clusters.size() == 1) {
      Cluster cluster = clusters.get(0);
      java.lang.Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = db.getSchema().getBucketByName(cluster.getClusterName()).getId();
      }
      if (clusterId == null) {
        throw new PCommandExecutionException("Cluster " + cluster + " does not exist");
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
        Cluster cluster = clusters.get(i);
        java.lang.Integer clusterId = cluster.getClusterNumber();
        if (clusterId == null) {
          clusterId = db.getSchema().getBucketByName(cluster.getClusterName()).getId();
        }
        if (clusterId == null) {
          throw new PCommandExecutionException("Cluster " + cluster + " does not exist");
        }
        clusterIds[i] = clusterId;
      }
      FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(clusterIds, ctx, orderByRidAsc, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(OSelectExecutionPlan plan, Statement subQuery, OCommandContext ctx, boolean profilingEnabled) {
    OBasicCommandContext subCtx = new OBasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    OInternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx, profilingEnabled));
  }

}
