/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

/**
 * <p>
 * This step is used just as a gate check for classes (eg. for CREATE EDGE to make sure that the passed type is an edge type).
 * </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckIsEdgeTypeStep extends AbstractExecutionStep {

  private final String targetClass;

  private long cost = 0;

  boolean found = false;

  /**
   * @param targetClass      a type to be checked
   * @param ctx              execuiton context
   * @param profilingEnabled true to collect execution stats
   */
  public CheckIsEdgeTypeStep(String targetClass, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;

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

      Schema schema = db.getSchema();

      DocumentType targettypez = schema.getType(this.targetClass);
      if (targettypez == null) {
        throw new CommandExecutionException("Type not found: " + this.targetClass);
      }

      if (targettypez instanceof EdgeType) {
        found = true;
      }
      if (!found) {
        throw new CommandExecutionException("Type  " + this.targetClass + " is not an Edge type");
      }

      return new InternalResultSet();
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
    result.append("+ CHECK USERTYPE HIERARCHY (E)");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
