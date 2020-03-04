/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

/**
 * Replaces all the occurrences.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodReplace extends OAbstractSQLMethod {

  public static final String NAME = "replace";

  public SQLMethodReplace() {
    super(NAME, 2, 2);
  }

  @Override
  public String getSyntax() {
    return "replace(<to-find>, <to-replace>)";
  }

  @Override
  public Object execute( final Object iThis, final Identifiable iCurrentRecord,
      final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (iThis == null || iParams[0] == null || iParams[1] == null)
      return iParams[0];

    return iThis.toString().replace(iParams[0].toString(), iParams[1].toString());
  }
}
