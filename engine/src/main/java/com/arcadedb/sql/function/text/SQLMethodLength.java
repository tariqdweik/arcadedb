/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

/**
 * Returns the string length.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodLength extends OAbstractSQLMethod {

  public static final String NAME = "length";

  public SQLMethodLength() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "length()";
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null)
      return 0;

    return iThis.toString().length();
  }
}
