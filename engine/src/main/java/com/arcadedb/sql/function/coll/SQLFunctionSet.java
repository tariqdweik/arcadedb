/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;

import java.util.HashSet;
import java.util.Set;

/**
 * This operator add an item in a set. The set doesn't accept duplicates, so adding multiple times the same value has no effect: the
 * value is contained only once.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionSet extends SQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "set";

  public SQLFunctionSet() {
    super(NAME, 1, -1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length > 1)
      // IN LINE MODE
      context = new HashSet<Object>();

    for (Object value : iParams) {
      if (value != null) {
        if (iParams.length == 1 && context == null)
          // AGGREGATION MODE (STATEFULL)
          context = new HashSet<Object>();

        if (value instanceof Document)
          context.add(value);
        else
          MultiValue.add(context, value);
      }
    }

    return context;
  }

  public String getSyntax() {
    return "set(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return configuredParameters.length == 1;
  }

}
