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
public class SQLMethodPrefix extends OAbstractSQLMethod {

  public static final String NAME = "prefix";

  public SQLMethodPrefix() {
    super(NAME, 1);
  }

  @Override
  public Object execute( Object iThis, Identifiable iRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null)
      return iThis;

    return iParams[0] + iThis.toString();
  }
}
