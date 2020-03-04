/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsInteger extends OAbstractSQLMethod {

  public static final String NAME = "asinteger";

  public SQLMethodAsInteger() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (ioResult instanceof Number) {
      ioResult = ((Number) ioResult).intValue();
    } else {
      ioResult = ioResult != null ? new Integer(ioResult.toString().trim()) : null;
    }
    return ioResult;
  }
}
