/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.schema.Type;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Collection;

/**
 * Compute the maximum value for a field. Uses the context to save the last maximum number. When different Number class are used,
 * take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionMax extends SQLFunctionMathAbstract {
  public static final String NAME = "max";

  private Object context;

  public SQLFunctionMax() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {

    // calculate max value for current record
    // consider both collection of parameters and collection in each parameter
    Object max = null;
    for (Object item : iParams) {
      if (item instanceof Collection<?>) {
        for (Object subitem : ((Collection<?>) item)) {
          if (max == null || subitem != null && ((Comparable) subitem).compareTo(max) > 0)
            max = subitem;
        }
      } else {
        if ((item instanceof Number) && (max instanceof Number)) {
          Number[] converted = Type.castComparableNumber((Number) item, (Number) max);
          item = converted[0];
          max = converted[1];
        }
        if (max == null || item != null && ((Comparable) item).compareTo(max) > 0)
          max = item;
      }
    }

    // what to do with the result, for current record, depends on how this function has been invoked
    // for an unique result aggregated from all output records
    if (aggregateResults() && max != null) {
      if (context == null)
        // FIRST TIME
        context = (Comparable) max;
      else {
        if (context instanceof Number && max instanceof Number) {
          final Number[] casted = Type.castComparableNumber((Number) context, (Number) max);
          context = casted[0];
          max = casted[1];
        }
        if (((Comparable<Object>) context).compareTo((Comparable) max) < 0)
          // BIGGER
          context = (Comparable) max;
      }

      return null;
    }

    // for non aggregated results (a result per output record)
    return max;
  }

  public boolean aggregateResults() {
    // LET definitions (contain $current) does not require results aggregation
    return ((configuredParameters.length == 1) && !configuredParameters[0].toString().contains("$current"));
  }

  public String getSyntax() {
    return "max(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return context;
  }
}
