/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.schema.OType;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Collection;

/**
 * Compute the minimum value for a field. Uses the context to save the last minimum number. When different Number class are used,
 * take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionMin extends SQLFunctionMathAbstract {
  public static final String NAME = "min";

  private Object context;

  public SQLFunctionMin() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {

    // calculate min value for current record
    // consider both collection of parameters and collection in each parameter
    Object min = null;
    for (Object item : iParams) {
      if (item instanceof Collection<?>) {
        for (Object subitem : ((Collection<?>) item)) {
          if (min == null || subitem != null && ((Comparable) subitem).compareTo(min) < 0)
            min = subitem;
        }
      } else {
        if ((item instanceof Number) && (min instanceof Number)) {
          Number[] converted = OType.castComparableNumber((Number) item, (Number) min);
          item = converted[0];
          min = converted[1];
        }
        if (min == null || item != null && ((Comparable) item).compareTo(min) < 0)
          min = item;
      }
    }

    // what to do with the result, for current record, depends on how this function has been invoked
    // for an unique result aggregated from all output records
    if (aggregateResults() && min != null) {
      if (context == null)
        // FIRST TIME
        context = (Comparable) min;
      else {
        if (context instanceof Number && min instanceof Number) {
          final Number[] casted = OType.castComparableNumber((Number) context, (Number) min);
          context = casted[0];
          min = casted[1];
        }

        if (((Comparable<Object>) context).compareTo((Comparable) min) > 0)
          // MINOR
          context = (Comparable) min;
      }

      return null;
    }

    // for non aggregated results (a result per output record)
    return min;
  }

  public boolean aggregateResults() {
    // LET definitions (contain $current) does not require results aggregation
    return ((configuredParameters.length == 1) && !configuredParameters[0].toString().contains("$current"));
  }

  public String getSyntax() {
    return "min(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return context;
  }
}
