package com.arcadedb.sql.executor;

import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.OCluster;

/**
 * <p> This step is used just as a gate check to verify that a cluster belongs to a class. </p> <p> It accepts two values: a target
 * cluster (name or OCluster) and a class. If the cluster belongs to the class, then the syncPool() returns an empty result set,
 * otherwise it throws an PCommandExecutionException </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  OCluster cluster;
  String   clusterName;

  String targetClass;

  private long cost = 0;

  boolean found = false;

  public CheckClusterTypeStep(String targetClusterName, String clazz, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterName = targetClusterName;
    this.targetClass = clazz;
  }

  public CheckClusterTypeStep(OCluster targetCluster, String clazz, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.cluster = targetCluster;
    this.targetClass = clazz;

  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (found) {
        return new OInternalResultSet();
      }
      PDatabase db = ctx.getDatabase();

      throw new UnsupportedOperationException("TODO");
//      int clusterId;
//      if (clusterName != null) {
//        clusterId = db.getClusterIdByName(clusterName);
//      } else if (cluster.getClusterName() != null) {
//        clusterId = db.getClusterIdByName(cluster.getClusterName());
//      } else {
//        clusterId = cluster.getClusterNumber();
//        if (db.getClusterNameById(clusterId) == null) {
//          throw new PCommandExecutionException("Cluster not found: " + clusterId);
//        }
//      }
//      if (clusterId < 0) {
//        throw new PCommandExecutionException("Cluster not found: " + clusterName);
//      }
//
//      OClass clazz = db.getMetadata().getSchema().getClass(targetClass);
//      if (clazz == null) {
//        throw new PCommandExecutionException("Class not found: " + targetClass);
//      }
//
//      for (int clust : clazz.getPolymorphicClusterIds()) {
//        if (clust == clusterId) {
//          found = true;
//          break;
//        }
//      }
//      if (!found) {
//        throw new PCommandExecutionException("Cluster " + clusterId + " does not belong to class " + targetClass);
//      }
//      return new OInternalResultSet();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR CLASS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append(spaces);
    result.append("  " + this.targetClass);
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
