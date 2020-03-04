/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.utility.FileUtils;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodLastIndexOf extends OAbstractSQLMethod {

  public static final String NAME = "lastindexof";

  public SQLMethodLastIndexOf() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    final String toFind = FileUtils.getStringContent(iParams[0].toString());
    return iParams.length > 1 ?
        iThis.toString().lastIndexOf(toFind, Integer.parseInt(iParams[1].toString())) :
        iThis.toString().lastIndexOf(toFind);
  }
}
