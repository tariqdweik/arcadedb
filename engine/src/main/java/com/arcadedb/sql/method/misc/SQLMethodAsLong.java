/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Date;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsLong extends OAbstractSQLMethod {

  public static final String NAME = "aslong";

  public SQLMethodAsLong() {
    super(NAME);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (ioResult instanceof Number) {
      ioResult = ((Number) ioResult).longValue();
    } else if (ioResult instanceof Date) {
      ioResult = ((Date) ioResult).getTime();
    } else {
      ioResult = ioResult != null ? new Long(ioResult.toString().trim()) : null;
    }
    return ioResult;
  }
}
