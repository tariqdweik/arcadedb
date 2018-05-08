/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

/**
 * Extracts a sub string from the original.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodSubString extends OAbstractSQLMethod {

  public static final String NAME = "substring";

  public SQLMethodSubString() {
    super(NAME, 1, 2);
  }

  @Override
  public String getSyntax() {
    return "subString(<from-index> [,<to-index>])";
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null) {
      return null;
    }

    if (iParams.length > 1) {
      int from = Integer.parseInt(iParams[0].toString());
      int to = Integer.parseInt(iParams[1].toString());
      String thisString = iThis.toString();
      if (from < 0) {
        from = 0;
      }
      if (from >= thisString.length()) {
        return "";
      }
      if (to > thisString.length()) {
        to = thisString.length();
      }
      if (to <= from) {
        return "";
      }

      return thisString.substring(from, to);
    } else {
      int from = Integer.parseInt(iParams[0].toString());
      String thisString = iThis.toString();
      if (from < 0) {
        from = 0;
      }
      if (from >= thisString.length()) {
        return "";
      }
      return thisString.substring(from);
    }
  }
}
