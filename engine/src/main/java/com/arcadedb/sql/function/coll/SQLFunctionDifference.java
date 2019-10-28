/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This operator can work inline. Returns the DIFFERENCE between the collections received as parameters. Works also with no
 * collection values.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionDifference extends SQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "difference";

  public SQLFunctionDifference() {
    super(NAME, 2, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams[0] == null)
      return null;

    // IN-LINE MODE (STATELESS)
    final Set<Object> result = new HashSet<Object>();

    boolean first = true;
    for (Object iParameter : iParams) {
      if (first) {
        if (iParameter instanceof Collection<?>) {
          result.addAll((Collection<Object>) iParameter);
        } else {
          result.add(iParameter);
        }
      } else {
        if (iParameter instanceof Collection<?>) {
          result.removeAll((Collection<Object>) iParameter);
        } else {
          result.remove(iParameter);
        }
      }

      first = false;
    }

    return result;

  }

  public String getSyntax() {
    return "difference(<field>, <field> [, <field]*)";
  }
}
