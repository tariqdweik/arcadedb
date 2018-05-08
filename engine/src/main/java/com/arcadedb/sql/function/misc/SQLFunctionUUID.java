/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.util.UUID;

/**
 * Generates a UUID as a 128-bits value using the Leach-Salz variant. For more information look at:
 * http://docs.oracle.com/javase/6/docs/api/java/util/UUID.html.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionUUID extends SQLFunctionAbstract {
  public static final String NAME = "uuid";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionUUID() {
    super(NAME, 0, 0);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    return UUID.randomUUID().toString();
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "uuid()";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
