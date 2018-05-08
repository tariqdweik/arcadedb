/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.utility.FileUtils;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodIndexOf extends OAbstractSQLMethod {

  public static final String NAME = "indexof";

  public SQLMethodIndexOf() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    final String toFind = FileUtils.getStringContent(iParams[0].toString());
    int startIndex = iParams.length > 1 ? Integer.parseInt(iParams[1].toString()) : 0;

    return iThis != null ? iThis.toString().indexOf(toFind, startIndex) : null;
  }
}
