package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.Json;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateContentStep extends AbstractExecutionStep {
  private final Json json;

  public UpdateContentStep(Json json, OCommandContext ctx, boolean profilingEnabled) {
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
        OResult result = upstream.next();
        if (result instanceof OResultInternal) {
          if (!(result.getElement().get() instanceof PRecord)) {
            ((OResultInternal) result).setElement((PDocument) result.getElement().get().getRecord());
          }
          if (!(result.getElement().get() instanceof PRecord)) {
            return result;
          }
          handleContent((PRecord) result.getElement().get(), ctx);
        }
        return result;
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

  private boolean handleContent(PRecord record, OCommandContext ctx) {
    boolean updated = false;

    final PModifiableDocument doc = (PModifiableDocument) record.getRecord().modify();

    doc.merge(json.toDocument(record, ctx));

    updated = true;

    return updated;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    result.append(json);
    return result.toString();
  }
}
