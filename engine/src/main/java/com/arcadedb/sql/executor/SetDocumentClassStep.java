/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Assigns a class to documents coming from upstream
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SetDocumentClassStep extends AbstractExecutionStep {
  private final String targetClass;

  public SetDocumentClassStep(Identifier targetClass, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass.getStringValue();
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

        if (result.isElement()) {
          Identifiable element = result.getElement().get().getRecord();
          if (element instanceof Record) {
            Record doc = (Record) element;
            if (!(result instanceof ResultInternal)) {
              result = new UpdatableResult((MutableDocument) doc);
            } else {
              ((ResultInternal) result).setElement((Document) doc);
            }
          }
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
    result.append("+ SET USERTYPE\n");
    result.append(spaces);
    result.append("  ");
    result.append(this.targetClass);
    return result.toString();
  }
}
