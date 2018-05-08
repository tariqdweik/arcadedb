/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Transforms a value to decimal. If the conversion is not possible, null is returned.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsDecimal extends OAbstractSQLMethod {

  public static final String NAME = "asdecimal";

  public SQLMethodAsDecimal() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "asDecimal()";
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis instanceof Date) {
      return new BigDecimal(((Date) iThis).getTime());
    }
    return iThis != null ? new BigDecimal(iThis.toString().trim()) : null;
  }
}
