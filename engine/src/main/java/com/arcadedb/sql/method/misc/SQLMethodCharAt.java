/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * Returns a character in a string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodCharAt extends OAbstractSQLMethod {

  public static final String NAME = "charat";

  public SQLMethodCharAt() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "charAt(<position>)";
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iParams[0] == null) {
      return null;
    }

    int index = Integer.parseInt(iParams[0].toString());
    return "" + iThis.toString().charAt(index);
  }
}
