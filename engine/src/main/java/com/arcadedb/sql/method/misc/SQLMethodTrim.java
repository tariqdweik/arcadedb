/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodTrim extends OAbstractSQLMethod {

  public static final String NAME = "trim";

  public SQLMethodTrim() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    ioResult = ioResult != null ? ioResult.toString().trim() : null;
    return ioResult;
  }
}
