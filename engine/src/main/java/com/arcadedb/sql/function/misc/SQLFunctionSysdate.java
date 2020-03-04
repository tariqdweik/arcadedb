/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Returns the current date time.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see SQLFunctionDate
 */
public class SQLFunctionSysdate extends SQLFunctionAbstract {
  public static final String NAME = "sysdate";

  private final Date             now;
  private       SimpleDateFormat format;

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionSysdate() {
    super(NAME, 0, 2);
    now = new Date();
  }

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length == 0)
      return now;

    if (format == null) {
      final TimeZone tz =
          iParams.length > 0 ? TimeZone.getTimeZone(iParams[0].toString()) : iContext.getDatabase().getSchema().getTimeZone();

      if (iParams.length > 1)
        format = new SimpleDateFormat((String) iParams[1]);
      else
        format = new SimpleDateFormat(iContext.getDatabase().getSchema().getDateFormat());

      format.setTimeZone(tz);
    }

    return format.format(now);
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "sysdate([<format>] [,<timezone>])";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
