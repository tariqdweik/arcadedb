/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Bucket;

/**
 * <p> This step is used just as a gate check to verify that a bucket belongs to a class. </p> <p> It accepts two values: a target
 * bucket (name or OCluster) and a class. If the bucket belongs to the class, then the syncPool() returns an empty result set,
 * otherwise it throws an PCommandExecutionException </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  Bucket bucket;
  String bucketName;

  String targetClass;

  private long cost = 0;

  boolean found = false;

  public CheckClusterTypeStep(String targetClusterName, String typez, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucketName = targetClusterName;
    this.targetClass = typez;
  }

  public CheckClusterTypeStep(Bucket targetCluster, String typez, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucket = targetCluster;
    this.targetClass = typez;

  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (found) {
        return new InternalResultSet();
      }
      Database db = ctx.getDatabase();

      throw new UnsupportedOperationException("TODO");
//      int bucketId;
//      if (bucketName != null) {
//        bucketId = db.getClusterIdByName(bucketName);
//      } else if (bucket.getClusterName() != null) {
//        bucketId = db.getClusterIdByName(bucket.getClusterName());
//      } else {
//        bucketId = bucket.getClusterNumber();
//        if (db.getClusterNameById(bucketId) == null) {
//          throw new PCommandExecutionException("Cluster not found: " + bucketId);
//        }
//      }
//      if (bucketId < 0) {
//        throw new PCommandExecutionException("Cluster not found: " + bucketName);
//      }
//
//      OClass typez = db.getMetadata().getSchema().getClass(targetClass);
//      if (typez == null) {
//        throw new PCommandExecutionException("Type not found: " + targetClass);
//      }
//
//      for (int clust : typez.getPolymorphicClusterIds()) {
//        if (clust == bucketId) {
//          found = true;
//          break;
//        }
//      }
//      if (!found) {
//        throw new PCommandExecutionException("Cluster " + bucketId + " does not belong to class " + targetClass);
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR USERTYPE");
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
