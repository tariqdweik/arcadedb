/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Extract the first item of multi values (arrays, collections and maps) or return the same value for non multi-value types.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionFirst extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "first";

  public SQLFunctionFirst() {
    super(NAME, 1, 1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    final Object value = iParams[0];

    if (MultiValue.isMultiValue(value))
      return MultiValue.getFirstValue(value);

    return null;
  }

  public String getSyntax() {
    return "first(<field>)";
  }
}
