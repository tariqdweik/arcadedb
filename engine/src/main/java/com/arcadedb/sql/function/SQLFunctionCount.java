/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.math.SQLFunctionMathAbstract;

/**
 * Count the record that contains a field. Use * to indicate the record instead of the field. Uses the context to save the counter
 * number. When different Number class are used, take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionCount extends SQLFunctionMathAbstract {
  public static final String NAME = "count";

  private long total = 0;

  public SQLFunctionCount() {
    super(NAME, 1, 1);
  }

  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {
    if (iParams.length == 0 || iParams[0] != null)
      total++;

    return total;
  }

  public boolean aggregateResults() {
    return true;
  }

  public String getSyntax() {
    return "count(<field>|*)";
  }

  @Override
  public Object getResult() {
    return total;
  }

  @Override
  public void setResult(final Object iResult) {
    total = ((Number) iResult).longValue();
  }
}
