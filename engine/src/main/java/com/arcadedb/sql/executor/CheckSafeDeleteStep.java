/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;

import java.util.Map;
import java.util.Optional;

/**
 * <p>
 * Checks if a record can be safely deleted (throws PCommandExecutionException in case).
 * A record cannot be safely deleted if it's a vertex or an edge (it requires additional operations).</p>
 * <p>
 * The result set returned by syncPull() throws an PCommandExecutionException as soon as it finds a record
 * that cannot be safely deleted (eg. a vertex or an edge)</p>
 * <p>This step is used used in DELETE statement to make sure that you are not deleting vertices or edges without passing for an
 * explicit DELETE VERTEX/EDGE</p>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CheckSafeDeleteStep extends AbstractExecutionStep {
  private long cost = 0;

  public CheckSafeDeleteStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result result = upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (result.isElement()) {

            Record record = result.getElement().get();
            if (record instanceof Document) {
              Document doc = (Document) record;
              DocumentType typez = ctx.getDatabase().getSchema().getType(doc.getType());
              //TODO
//              if (typez != null) {
//                if (typez.getName().equalsIgnoreCase("V") || typez.isSubClassOf("V")) {
//                  throw new PCommandExecutionException("Cannot safely delete a vertex, please use DELETE VERTEX or UNSAFE");
//                }
//                if (typez.getName().equalsIgnoreCase("E") || typez.isSubClassOf("E")) {
//                  throw new PCommandExecutionException("Cannot safely delete an edge, please use DELETE EDGE or UNSAFE");
//                }
//              }
            }
          }
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
      public Optional<ExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK SAFE DELETE");
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
