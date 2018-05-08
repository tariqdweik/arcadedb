/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;
import com.arcadedb.utility.FileUtils;

/**
 * Appends strings. Acts as a concatenation.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAppend extends OAbstractSQLMethod {

  public static final String NAME = "append";

  public SQLMethodAppend() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "append([<value|expression|field>]*)";
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null)
      return iThis;

    final StringBuilder buffer = new StringBuilder(iThis.toString());
    for (int i = 0; i < iParams.length; ++i) {
      if (iParams[i] != null) {
        buffer.append(FileUtils.getStringContent(iParams[i]));
      }
    }

    return buffer.toString();
  }

}
