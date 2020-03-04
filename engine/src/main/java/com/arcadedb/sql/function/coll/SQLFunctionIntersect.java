/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;

import java.util.*;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the INTERSECTION of the collections received as parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionIntersect extends SQLFunctionMultiValueAbstract<Object> {
  public static final String NAME = "intersect";

  public SQLFunctionIntersect() {
    super(NAME, 1, -1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    Object value = iParams[0];

    if (value == null)
      return Collections.emptySet();

    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      if (context == null) {
        // ADD ALL THE ITEMS OF THE FIRST COLLECTION
        if (value instanceof Collection) {
          context = ((Collection) value).iterator();
        } else if (value instanceof Iterator) {
          context = value;
        } else if (value instanceof Iterable) {
          context = ((Iterable) value).iterator();
        } else {
          context = Arrays.asList(value).iterator();
        }
      } else {
        Iterator contextIterator = null;
        if (context instanceof Iterator) {
          contextIterator = (Iterator) context;
        } else if (MultiValue.isMultiValue(context)) {
          contextIterator = MultiValue.getMultiValueIterator(context);
        }
        context = intersectWith(contextIterator, value);
      }
      return null;
    }

    // IN-LINE MODE (STATELESS)
    Iterator iterator = MultiValue.getMultiValueIterator(value, false);

    for (int i = 1; i < iParams.length; ++i) {
      value = iParams[i];

      if (value != null) {
        value = intersectWith(iterator, value);
        iterator = MultiValue.getMultiValueIterator(value, false);
      } else {
        return new ArrayList().iterator();
      }
    }

    List result = new ArrayList();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
    return result;
  }

  @Override
  public Object getResult() {
    return MultiValue.toSet(context);
  }

  static Collection intersectWith(final Iterator current, Object value) {
    final HashSet tempSet = new HashSet();

    if (!(value instanceof Set))
      value = MultiValue.toSet(value);

    for (Iterator it = current; it.hasNext(); ) {
      final Object curr = it.next();

      if (value instanceof Collection) {
        if (((Collection) value).contains(curr))
          tempSet.add(curr);
      }
    }

    return tempSet;
  }

  public String getSyntax() {
    return "intersect(<field>*)";
  }
}
