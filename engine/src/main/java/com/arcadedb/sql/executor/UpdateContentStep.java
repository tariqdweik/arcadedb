package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
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

//    // REPLACE ALL THE CONTENT
//    final ODocument fieldsToPreserve = new ODocument();
//
//    final OClass restricted = ctx.getDatabase().getMetadata().getSchema().getClass(OSecurity.RESTRICTED_CLASSNAME);
//
//    if (restricted != null && restricted.isSuperClassOf(record.getSchemaType().orElse(null))) {
//      for (OProperty prop : restricted.properties()) {
//        fieldsToPreserve.field(prop.getName(), record.<Object>getProperty(prop.getName()));
//      }
//    }
//
//    OClass recordClass = ODocumentInternal.getImmutableSchemaClass(record.getRecord());
//    if (recordClass != null && recordClass.isSubClassOf("V")) {
//      for (String fieldName : record.getPropertyNames()) {
//        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
//          fieldsToPreserve.field(fieldName, record.<Object>getProperty(fieldName));
//        }
//      }
//    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
//      for (String fieldName : record.getPropertyNames()) {
//        if (fieldName.equals("in") || fieldName.equals("out")) {
//          fieldsToPreserve.field(fieldName, record.<Object>getProperty(fieldName));
//        }
//      }
//    }
//    ODocument doc = record.getRecord();
//    doc.merge(json.toDocument(record, ctx), false, false);
//    doc.merge(fieldsToPreserve, true, false);
//
//    updated = true;
//
//    return updated;
    throw new UnsupportedOperationException();
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
