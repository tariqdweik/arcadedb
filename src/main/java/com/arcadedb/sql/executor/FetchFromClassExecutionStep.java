package com.arcadedb.sql.executor;

import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.schema.PDocumentType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class FetchFromClassExecutionStep extends AbstractExecutionStep {

  private String className;
  private boolean              orderByRidAsc  = false;
  private boolean              orderByRidDesc = false;
  private List<OExecutionStep> subSteps       = new ArrayList<>();

  OResultSet currentResultSet;
  int currentStep = 0;

  protected FetchFromClassExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  public FetchFromClassExecutionStep(String className, Set<String> clusters, OCommandContext ctx, Boolean ridOrder,
      boolean profilingEnabled) {
    this(className, clusters, null, ctx, ridOrder, profilingEnabled);
  }

  /**
   * iterates over a class and its subclasses
   *
   * @param className the class name
   * @param clusters  if present (it can be null), filter by only these clusters
   * @param ctx       the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClassExecutionStep(String className, Set<String> clusters, QueryPlanningInfo planningInfo, OCommandContext ctx,
      Boolean ridOrder, boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.className = className;

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }
    PDocumentType clazz = ctx.getDatabase().getSchema().getType(className);
    if (clazz == null) {
      throw new PCommandExecutionException("Class " + className + " not found");
    }
    int[] classClusters = clazz.getBuckets().stream().mapToInt(x -> x.getId()).toArray();
    List<Integer> filteredClassClusters = new ArrayList<>();
    for (int clusterId : classClusters) {
      String clusterName = ctx.getDatabase().getSchema().getBucketById(clusterId).getName();
      if (clusters == null || clusters.contains(clusterName)) {
        filteredClassClusters.add(clusterId);
      }
    }
    int[] clusterIds = new int[filteredClassClusters.size() + 1];
    for (int i = 0; i < filteredClassClusters.size(); i++) {
      clusterIds[i] = filteredClassClusters.get(i);
    }
    clusterIds[clusterIds.length - 1] = -1;//temporary cluster, data in tx

    sortClusers(clusterIds);
    for (int i = 0; i < clusterIds.length; i++) {
      int clusterId = clusterIds[i];
      if (clusterId > 0) {
        FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterId, planningInfo, ctx, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        getSubSteps().add(step);
      } else {
        //current tx
        FetchTemporaryFromTxStep step = new FetchTemporaryFromTxStep(ctx, className, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        getSubSteps().add(step);
      }
    }
  }

  private void sortClusers(int[] clusterIds) {
    if (orderByRidAsc) {
      Arrays.sort(clusterIds);
    } else if (orderByRidDesc) {
      Arrays.sort(clusterIds);
      //revert order
      for (int i = 0; i < clusterIds.length / 2; i++) {
        int old = clusterIds[i];
        clusterIds[i] = clusterIds[clusterIds.length - 1 - i];
        clusterIds[clusterIds.length - 1 - i] = old;
      }
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {

      int totDispatched = 0;

      @Override
      public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords) {
            return false;
          }
          if (currentResultSet != null && currentResultSet.hasNext()) {
            return true;
          } else {
            if (currentStep >= getSubSteps().size()) {
              return false;
            }
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
        }
      }

      @Override
      public OResult next() {
        while (true) {
          if (totDispatched >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentResultSet != null && currentResultSet.hasNext()) {
            totDispatched++;
            OResult result = currentResultSet.next();
            ctx.setVariable("$current", result);
            return result;
          } else {
            if (currentStep >= getSubSteps().size()) {
              throw new IllegalStateException();
            }
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
        }
      }

      @Override
      public void close() {
        for (OExecutionStep step : getSubSteps()) {
          ((AbstractExecutionStep) step).close();
        }
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return new HashMap<>();
      }
    };

  }

  @Override
  public void sendTimeout() {
    for (OExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).sendTimeout();
    }
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    for (OExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).close();
    }
    prev.ifPresent(p -> p.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM CLASS " + className);
    if (profilingEnabled) {
      builder.append(" (" + getCostFormatted() + ")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      OExecutionStepInternal step = (OExecutionStepInternal) getSubSteps().get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public long getCost() {
    return getSubSteps().stream().map(x -> x.getCost()).reduce((a, b) -> a + b).orElse(0L);
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("className", className);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.className = fromResult.getProperty("className");
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw new PCommandExecutionException("", e);
    }
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    FetchFromClassExecutionStep result = new FetchFromClassExecutionStep(ctx, profilingEnabled);
    result.className = this.className;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps = this.subSteps.stream().map(x -> ((OExecutionStepInternal) x).copy(ctx)).collect(Collectors.toList());
    return result;
  }
}

