package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC  = "ASC";
  public static final Object ORDER_DESC = "DESC";
  private final QueryPlanningInfo queryPlanning;

  private int    clusterId;
  private Object order;

  private Iterator<PRecord> iterator;
  private long cost = 0;

  public FetchFromClusterExecutionStep(int clusterId, OCommandContext ctx, boolean profilingEnabled) {
    this(clusterId, null, ctx, profilingEnabled);
  }

  public FetchFromClusterExecutionStep(int clusterId, QueryPlanningInfo queryPlanning, OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterId = clusterId;
    this.queryPlanning = queryPlanning;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        long minClusterPosition = calculateMinClusterPosition();
        long maxClusterPosition = calculateMaxClusterPosition();
        iterator = ctx.getDatabase().getSchema().getBucketById(clusterId).iterator();

        //TODO check how to support ranges and DESC
//            new ORecordIteratorCluster((ODatabaseDocumentInternal) ctx.getDatabase(),
//            (ODatabaseDocumentInternal) ctx.getDatabase(), clusterId, minClusterPosition, maxClusterPosition);
//        if (ORDER_DESC == order) {
//          iterator.last();
//        }
      }
      OResultSet rs = new OResultSet() {

        int nFetched = 0;

        @Override
        public boolean hasNext() {
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords) {
              return false;
            }
            //TODO
//            if (ORDER_DESC == order) {
//              return iterator.hasPrevious();
//            } else {
              return iterator.hasNext();
//            }
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }

        @Override
        public OResult next() {
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords) {
              throw new IllegalStateException();
            }
//            if (ORDER_DESC == order && !iterator.hasPrevious()) {
//              throw new IllegalStateException();
//            } else
              if (!iterator.hasNext()) {
              throw new IllegalStateException();
            }

            PRecord record = null;
//            if (ORDER_DESC == order) {
//              record = iterator.previous();
//            } else {
              record = iterator.next();
//            }
            nFetched++;
            OResultInternal result = new OResultInternal();
            result.element = (PDocument) record;
            ctx.setVariable("$current", result);
            return result;
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
      return rs;
    } catch (IOException e) {
      throw new PCommandExecutionException(e);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }

  }

  private long calculateMinClusterPosition() {
    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }

    long maxValue = -1;

    for (BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof BinaryCondition) {
        BinaryCondition cond = (BinaryCondition) ridRangeCondition;
        Rid condRid = cond.getRight().getRid();
        BinaryCompareOperator operator = cond.getOperator();
        if (condRid != null) {
          if (condRid.getCluster().getValue().intValue() != this.clusterId) {
            continue;
          }
          if (operator instanceof GtOperator || operator instanceof GeOperator) {
            maxValue = Math.max(maxValue, condRid.getPosition().getValue().longValue());
          }
        }
      }
    }

    return maxValue;
  }

  private long calculateMaxClusterPosition() {
    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }
    long minValue = Long.MAX_VALUE;

    for (BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof BinaryCondition) {
        BinaryCondition cond = (BinaryCondition) ridRangeCondition;
        PRID conditionRid;

        Object obj;
        if (((BinaryCondition) ridRangeCondition).getRight().getRid() != null) {
          obj = ((BinaryCondition) ridRangeCondition).getRight().getRid().toRecordId((OResult) null, ctx);
        } else {
          obj = ((BinaryCondition) ridRangeCondition).getRight().execute((OResult) null, ctx);
        }

        conditionRid = ((PIdentifiable) obj).getIdentity();
        BinaryCompareOperator operator = cond.getOperator();
        if (conditionRid != null) {
          if (conditionRid.getBucketId() != this.clusterId) {
            continue;
          }
          if (operator instanceof LtOperator || operator instanceof LeOperator) {
            minValue = Math.min(minValue, conditionRid.getPosition());
          }
        }
      }
    }

    return minValue == Long.MAX_VALUE ? -1 : minValue;
  }

  @Override
  public void sendTimeout() {
    super.sendTimeout();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM CLUSTER " + clusterId + " " + (ORDER_DESC.equals(order) ?
            "DESC" :
            "ASC");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("clusterId", clusterId);
    result.setProperty("order", order);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.clusterId = fromResult.getProperty("clusterId");
      Object orderProp = fromResult.getProperty("order");
      if (orderProp != null) {
        this.order = ORDER_ASC.equals(fromResult.getProperty("order")) ? ORDER_ASC : ORDER_DESC;
      }
    } catch (Exception e) {
      throw new PCommandExecutionException(e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    FetchFromClusterExecutionStep result = new FetchFromClusterExecutionStep(this.clusterId,
        this.queryPlanning == null ? null : this.queryPlanning.copy(), ctx, profilingEnabled);
    return result;
  }
}
