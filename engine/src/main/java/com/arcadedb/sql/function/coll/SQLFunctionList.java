/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This operator add an item in a list. The list accepts duplicates.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionList extends SQLFunctionMultiValueAbstract<List<Object>> {
  public static final String NAME = "list";

  public SQLFunctionList() {
    super(NAME, 1, -1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length > 1)
      // IN LINE MODE
      context = new ArrayList<>();

    for (Object value : iParams) {
      if (value != null) {
        if (iParams.length == 1 && context == null)
          // AGGREGATION MODE (STATEFULL)
          context = new ArrayList<Object>();

        if (value instanceof Map)
          context.add(value);
        else
          MultiValue.add(context, value);
      }
    }
    return context;
  }

  public String getSyntax() {
    return "list(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }
}
