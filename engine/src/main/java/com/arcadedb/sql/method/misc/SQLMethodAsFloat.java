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
public class SQLMethodAsFloat extends OAbstractSQLMethod {

  public static final String NAME = "asfloat";

  public SQLMethodAsFloat() {
    super(NAME);
  }

  @Override
  public Object execute( final Object iThis, final Identifiable iCurrentRecord,
      final CommandContext iContext, Object ioResult, final Object[] iParams) {
    if (ioResult instanceof Number)
      ioResult = ((Number) ioResult).floatValue();
    else
      ioResult = ioResult != null ? new Float(ioResult.toString().trim()) : null;

    return ioResult;
  }
}
