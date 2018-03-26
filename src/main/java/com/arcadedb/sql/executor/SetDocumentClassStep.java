package com.arcadedb.sql.executor;

import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Assigns a class to documents coming from upstream
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class SetDocumentClassStep extends AbstractExecutionStep {
  private final String targetClass;

  public SetDocumentClassStep(OIdentifier targetClass, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass.getStringValue();
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
        throw new UnsupportedOperationException();
//        if (result.isElement()) {
//          PIdentifiable element = result.getElement().get().getRecord();
//          if (element instanceof PRecord) {
//            PRecord doc = (PRecord) element;
//            doc.setClassName(targetClass);
//            if (!(result instanceof OResultInternal)) {
//              result = new OUpdatableResult(doc);
//            } else {
//              ((OResultInternal) result).setElement(doc);
//            }
//          }
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET CLASS\n");
    result.append(spaces);
    result.append("  ");
    result.append(this.targetClass);
    return result.toString();
  }
}
