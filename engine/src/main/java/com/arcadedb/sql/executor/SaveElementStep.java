/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SaveElementStep extends AbstractExecutionStep {

  private final Identifier bucket;

  public SaveElementStep(CommandContext ctx, Identifier bucket, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucket = bucket;
  }

  public SaveElementStep(CommandContext ctx, boolean profilingEnabled) {
    this(ctx, null, profilingEnabled);
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
          final Document doc = result.getElement().orElse(null);

          final MutableDocument modifiableDoc = (MutableDocument) doc.modify();
          if (bucket == null)
            modifiableDoc.save();
          else
            modifiableDoc.save(bucket.getStringValue());
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
    result.append("+ SAVE RECORD");
    if (bucket != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on bucket " + bucket);
    }
    return result.toString();
  }
}
