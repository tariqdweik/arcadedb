/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Extract the last item of multi values (arrays, collections and maps) or return the same value for non multi-value types.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionLast extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "last";

  public SQLFunctionLast() {
    super(NAME, 1, 1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    final Object value = iParams[0];

    if (MultiValue.isMultiValue(value))
      return MultiValue.getLastValue(value);

    return null;
  }

  public String getSyntax() {
    return "last(<field>)";
  }
}
