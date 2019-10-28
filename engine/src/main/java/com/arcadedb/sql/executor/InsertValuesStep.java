/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.Identifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class InsertValuesStep extends AbstractExecutionStep {
  private final List<Identifier>       identifiers;
  private final List<List<Expression>> values;

  int nextValueSet = 0;

  public InsertValuesStep(List<Identifier> identifierList, List<List<Expression>> valueExpressions, CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.identifiers = identifierList;
    this.values = valueExpressions;
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
        if (!(result instanceof ResultInternal)) {
          if (!result.isElement()) {
            throw new CommandExecutionException("Error executing INSERT, cannot modify element: " + result);
          }
          result = new UpdatableResult((MutableDocument) result.getElement().get());
        }
        List<Expression> currentValues = values.get(nextValueSet++);
        if (currentValues.size() != identifiers.size()) {
          throw new CommandExecutionException(
              "Cannot execute INSERT, the number of fields is different from the number of expressions: " + identifiers + " "
                  + currentValues);
        }
        nextValueSet %= values.size();
        for (int i = 0; i < currentValues.size(); i++) {
          Identifier identifier = identifiers.get(i);
          Object value = currentValues.get(i).execute(result, ctx);
          ((ResultInternal) result).setProperty(identifier.getStringValue(), value);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
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
    result.append("+ SET VALUES \n");
    result.append(spaces);
    result.append("  (");
    for (int i = 0; i < identifiers.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      result.append(identifiers.get(i));
    }
    result.append(")\n");

    result.append(spaces);
    result.append("  VALUES\n");

    for (int c = 0; c < this.values.size(); c++) {
      if (c > 0) {
        result.append("\n");
      }
      List<Expression> exprs = this.values.get(c);
      result.append(spaces);
      result.append("  (");
      for (int i = 0; i < exprs.size() && i < 3; i++) {
        if (i > 0) {
          result.append(", ");
        }
        result.append(exprs.get(i));
      }
      result.append(")");
    }
    if (this.values.size() >= 3) {
      result.append(spaces);
      result.append("  ...");

    }
    return result.toString();
  }
}
