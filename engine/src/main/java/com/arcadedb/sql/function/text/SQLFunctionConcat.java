/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;

public class SQLFunctionConcat extends SQLFunctionConfigurableAbstract {
  public static final String        NAME = "concat";
  private             StringBuilder sb;

  public SQLFunctionConcat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      Object[] iParams, CommandContext iContext) {
    if (sb == null) {
      sb = new StringBuilder();
    } else {
      if (iParams.length > 1)
        sb.append(iParams[1]);
    }
    sb.append(iParams[0]);
    return null;
  }

  @Override
  public Object getResult() {
    return sb != null ? sb.toString() : null;
  }

  @Override
  public String getSyntax() {
    return "concat(<field>, [<delim>])";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

}
