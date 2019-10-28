/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodSize extends OAbstractSQLMethod {

  public static final String NAME = "size";

  public SQLMethodSize() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult,
      final Object[] iParams) {

    final Number size;
    if (ioResult != null) {
      if (ioResult instanceof Identifiable) {
        size = 1;
      } else {
        size = MultiValue.getSize(ioResult);
      }
    } else {
      size = 0;
    }

    return size;
  }
}
