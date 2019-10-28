/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * Returns the first characters from the beginning of the string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodLeft extends OAbstractSQLMethod {

  public static final String NAME = "left";

  public SQLMethodLeft() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "left(<characters>)";
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (iParams[0] == null || iThis == null)
      return null;

    final String valueAsString = iThis.toString();

    final int len = Integer.parseInt(iParams[0].toString());
    return valueAsString.substring(0, len <= valueAsString.length() ? len : valueAsString.length());
  }
}
