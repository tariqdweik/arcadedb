package com.arcadedb.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Json;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateMergeStep extends AbstractExecutionStep {
  private final Json json;

  public UpdateMergeStep(Json json, CommandContext ctx, boolean profilingEnabled) {
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
        throw new UnsupportedOperationException();
//        OResult result = upstream.next();
//        if (result instanceof OResultInternal) {
//          if (!(result.getElement().orElse(null) instanceof ODocument)) {
//            ((OResultInternal) result).setElement(result.getElement().get().getRecord());
//          }
//          if (!(result.getElement().orElse(null) instanceof ODocument)) {
//            return result;
//          }
//          handleMerge((ODocument) result.getElement().orElse(null), ctx);
//        }
//        return result;
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

  private void handleMerge(Record record, CommandContext ctx) {
    throw new UnsupportedOperationException();
//    record.merge(json.toDocument(record, ctx), true, false);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE MERGE\n");
    result.append(spaces);
    result.append("  ");
    result.append(json);
    return result.toString();
  }
}
