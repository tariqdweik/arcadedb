package com.arcadedb.sql.executor;

import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Returns the number of records contained in a class (including subclasses) Executes a count(*) on a class and returns a single
 * record that contains that value (with a specific alias).
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromClassStep extends AbstractExecutionStep {
  private final Identifier target;
  private final String     alias;

  private long cost = 0;

  private boolean executed = false;

  /**
   * @param targetClass      An identifier containing the name of the class to count
   * @param alias            the name of the property returned in the result-set
   * @param ctx              the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromClassStep(Identifier targetClass, String alias, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetClass;
    this.alias = alias;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public OResult next() {
        if (executed) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          PDocumentType clazz = ctx.getDatabase().getSchema().getType(target.getStringValue());
          if (clazz == null) {
            throw new PCommandExecutionException("Class " + target.getStringValue() + " does not exist in the database schema");
          }
          //TODO
          throw new UnsupportedOperationException("TODO");
//          long size = clazz.count();
//          executed = true;
//          OResultInternal result = new OResultInternal();
//          result.setProperty(alias, size);
//          return result;
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

      @Override
      public void reset() {
        CountFromClassStep.this.reset();
      }
    };
  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE CLASS SIZE: " + target;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

}
