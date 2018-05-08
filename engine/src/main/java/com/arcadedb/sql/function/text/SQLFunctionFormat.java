/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

/**
 * Formats content.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionFormat extends SQLFunctionAbstract {
  public static final String NAME = "format";

  public SQLFunctionFormat() {
    super(NAME, 1, -1);
  }

  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] params, CommandContext iContext) {
    final Object[] args = new Object[params.length - 1];

    for (int i = 0; i < args.length; ++i)
      args[i] = params[i + 1];

    return String.format((String) params[0], args);
  }

  public String getSyntax() {
    return "format(<format>, <arg1> [,<argN>]*)";
  }
}
