package com.arcadedb.sql.executor;

import com.arcadedb.database.PBaseDocument;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.schema.PType;

import java.util.Map;
import java.util.Optional;

/**
 * <p>
 * Checks if a record can be safely deleted (throws OCommandExecutionException in case).
 * A record cannot be safely deleted if it's a vertex or an edge (it requires additional operations).</p>
 * <p>
 * The result set returned by syncPull() throws an OCommandExecutionException as soon as it finds a record
 * that cannot be safely deleted (eg. a vertex or an edge)</p>
 * <p>This step is used used in DELETE statement to make sure that you are not deleting vertices or edges without passing for an
 * explicit DELETE VERTEX/EDGE</p>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CheckSafeDeleteStep extends AbstractExecutionStep {
  private long cost = 0;

  public CheckSafeDeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (result.isElement()) {

            PRecord record = result.getElement().get();
            if (record instanceof PBaseDocument) {
              PBaseDocument doc = (PBaseDocument) record;
              PType clazz = ctx.getDatabase().getSchema().getType(doc.getType());
              //TODO
//              if (clazz != null) {
//                if (clazz.getName().equalsIgnoreCase("V") || clazz.isSubClassOf("V")) {
//                  throw new OCommandExecutionException("Cannot safely delete a vertex, please use DELETE VERTEX or UNSAFE");
//                }
//                if (clazz.getName().equalsIgnoreCase("E") || clazz.isSubClassOf("E")) {
//                  throw new OCommandExecutionException("Cannot safely delete an edge, please use DELETE EDGE or UNSAFE");
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
      public Optional<OExecutionPlan> getExecutionPlan() {
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
