/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Returns the number of records contained in a class (including subTypes) Executes a count(*) on a class and returns a single
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
  public CountFromClassStep(Identifier targetClass, String alias, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetClass;
    this.alias = alias;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          DocumentType typez = ctx.getDatabase().getSchema().getType(target.getStringValue());
          if (typez == null) {
            throw new CommandExecutionException("Type " + target.getStringValue() + " does not exist in the database schema");
          }

          long size = ctx.getDatabase().countType(target.getStringValue(), true);
          executed = true;
          ResultInternal result = new ResultInternal();
          result.setProperty(alias, size);
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE USERTYPE SIZE: " + target;
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
