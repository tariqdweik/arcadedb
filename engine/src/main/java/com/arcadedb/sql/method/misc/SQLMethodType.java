/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.schema.OType;
import com.arcadedb.sql.executor.CommandContext;

/**
 * Returns the value's OrientDB Type.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodType extends OAbstractSQLMethod {

  public static final String NAME = "type";

  public SQLMethodType() {
    super(NAME);
  }

  @Override
  public Object execute( final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext,
      final Object ioResult, final Object[] iParams) {
    if (ioResult == null)
      return null;

    final OType t = OType.getTypeByValue(ioResult);

    if (t != null)
      return t.toString();

    return null;
  }
}
