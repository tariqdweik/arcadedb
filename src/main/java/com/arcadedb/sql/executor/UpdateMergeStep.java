package com.arcadedb.sql.executor;

import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.OJson;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateMergeStep extends AbstractExecutionStep {
  private final OJson json;

  public UpdateMergeStep(OJson json, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
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
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void handleMerge(PRecord record, OCommandContext ctx) {
    throw new UnsupportedOperationException();
//    record.merge(json.toDocument(record, ctx), true, false);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE MERGE\n");
    result.append(spaces);
    result.append("  ");
    result.append(json);
    return result.toString();
  }
}
