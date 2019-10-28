/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * Splits a string using a delimiter.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodSplit extends OAbstractSQLMethod {

  public static final String NAME = "split";

  public SQLMethodSplit() {
    super(NAME, 1);
  }

  @Override
  public Object execute( Object iThis, Identifiable iRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null)
      return iThis;

    return iThis.toString().split(iParams[0].toString());
  }
}
