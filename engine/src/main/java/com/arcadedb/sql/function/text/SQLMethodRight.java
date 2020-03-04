/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

/**
 * Returns the first characters from the end of the string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodRight extends OAbstractSQLMethod {

  public static final String NAME = "right";

  public SQLMethodRight() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "right( <characters>)";
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null) {
      return null;
    }

    final String valueAsString = iThis.toString();

    final int offset = Integer.parseInt(iParams[0].toString());
    return valueAsString.substring(offset < valueAsString.length() ? valueAsString.length() - offset : 0);
  }

}
