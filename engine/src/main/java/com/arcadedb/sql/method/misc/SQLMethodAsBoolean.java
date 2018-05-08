/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsBoolean extends OAbstractSQLMethod {

  public static final String NAME = "asboolean";

  public SQLMethodAsBoolean() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (ioResult != null) {
      if (ioResult instanceof String) {
        ioResult = Boolean.valueOf(((String) ioResult).trim());
      } else if (ioResult instanceof Number) {
        return ((Number) ioResult).intValue() != 0;
      }
    }
    return ioResult;
  }
}
