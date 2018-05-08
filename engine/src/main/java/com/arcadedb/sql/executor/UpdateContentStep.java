package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Json;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateContentStep extends AbstractExecutionStep {
  private final Json json;

  public UpdateContentStep(Json json, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;

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
        if (result instanceof ResultInternal) {
          if (!(result.getElement().get() instanceof Record)) {
            ((ResultInternal) result).setElement((Document) result.getElement().get().getRecord());
          }
          if (!(result.getElement().get() instanceof Record)) {
            return result;
          }
          handleContent((Record) result.getElement().get(), ctx);
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

  private boolean handleContent(Record record, CommandContext ctx) {
    boolean updated = false;

    final ModifiableDocument doc = (ModifiableDocument) record.getRecord().modify();

    doc.merge(json.toDocument(record, ctx));

    updated = true;

    return updated;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    result.append(json);
    return result.toString();
  }
}
