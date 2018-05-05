package com.arcadedb.sql.executor;

import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PSchema;

/**
 * <p>
 * This step is used just as a gate check for classes (eg. for CREATE VERTEX to make sure that the passed class is a vertex class).
 * </p>
 * <p>
 * It accepts two values: a target class and a parent class. If the two classes are the same or if the parent class is indeed
 * a parent class of the target class, then the syncPool() returns an empty result set, otherwise it throws an PCommandExecutionException
 * </p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckClassTypeStep extends AbstractExecutionStep {

  private final String targetClass;
  private final String parentClass;

  private long cost = 0;

  boolean found = false;

  /**
   * @param targetClass      a class to be checked
   * @param parentClass      a class that is supposed to be the same or a parent class of the target class
   * @param ctx              execuiton context
   * @param profilingEnabled true to collect execution stats
   */
  public CheckClassTypeStep(String targetClass, String parentClass, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.parentClass = parentClass;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (found) {
        return new OInternalResultSet();
      }
      if (this.targetClass.equals(this.parentClass)) {
        return new OInternalResultSet();
      }

      if (this.parentClass != null) {
        if (!this.parentClass.equals("V") && this.parentClass.equals("E")) {
          PDatabase db = ctx.getDatabase();

          PSchema schema = db.getSchema();
          PDocumentType parentClazz = schema.getType(this.parentClass);

          PDocumentType targetClazz = schema.getType(this.targetClass);
          if (targetClazz == null) {
            throw new PCommandExecutionException("Class not found: " + this.targetClass);
          }

          if (parentClazz.equals(targetClazz)) {
            found = true;
          } else {
            for (PDocumentType sublcass : parentClazz.getSubTypes()) {
              if (sublcass.equals(targetClazz)) {
                this.found = true;
                break;
              }
            }
          }
          if (!found) {
            throw new PCommandExecutionException("Class  " + this.targetClass + " is not a subclass of " + this.parentClass);
          }
        }
      }
      return new OInternalResultSet();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK CLASS HIERARCHY");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append("  " + this.parentClass);
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
