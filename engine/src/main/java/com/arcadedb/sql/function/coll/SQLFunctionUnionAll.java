/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.utility.MultiIterator;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns, a
 * UNION of the collections received as parameters. Works also with no collection values. Does not remove duplication from the
 * result.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionUnionAll extends SQLFunctionMultiValueAbstract<Collection<Object>> {
  public static final String NAME = "unionAll";

  public SQLFunctionUnionAll() {
    super(NAME, 1, -1);
  }

  public Object execute( final Object iThis, final Identifiable iCurrentRecord,
      final Object iCurrentResult, final Object[] iParams, CommandContext iContext) {
    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      Object value = iParams[0];
      if (value != null) {

        if (context == null)
          context = new ArrayList<Object>();

        MultiValue.add(context, value);
      }

      return context;
    } else {
      // IN-LINE MODE (STATELESS)
      final MultiIterator<Identifiable> result = new MultiIterator<>();
      for (Object value : iParams) {
        if (value != null)
          result.add(value);
      }

      return result;
    }
  }

  public String getSyntax() {
    return "unionAll(<field>*)";
  }
}
