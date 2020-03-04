/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Locale;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodToUpperCase extends OAbstractSQLMethod {

  public static final String NAME = "touppercase";

  public SQLMethodToUpperCase() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    ioResult = ioResult != null ? ioResult.toString().toUpperCase(Locale.ENGLISH) : null;
    return ioResult;
  }
}
