/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

/**
 * Transforms a value to datetime. If the conversion is not possible, null is returned.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsDateTime extends OAbstractSQLMethod {

  public static final String NAME = "asdatetime";

  public SQLMethodAsDateTime() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "asDatetime()";
  }

  @Override
  public Object execute(Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis != null) {
      if (iThis instanceof Date) {
        return iThis;
      } else if (iThis instanceof Number) {
        return new Date(((Number) iThis).longValue());
      } else {
        try {
          return new SimpleDateFormat(iContext.getDatabase().getSchema().getDateTimeFormat()).parse(iThis.toString());
        } catch (ParseException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error during %s execution", e, NAME);
          // IGNORE IT: RETURN NULL
        }
      }
    }
    return null;
  }
}
