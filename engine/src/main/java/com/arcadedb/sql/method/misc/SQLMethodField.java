/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodField extends OAbstractSQLMethod {

  public static final String NAME = "field";

  public SQLMethodField() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute( Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, Object ioResult,
      final Object[] iParams) {
    if (iParams[0] == null)
      return null;

    final String paramAsString = iParams[0].toString();

    if (ioResult instanceof Identifiable) {
      final Document doc = (Document) ((Identifiable) ioResult).getRecord();
      return doc.get(paramAsString);
    }

    return null;
  }

  @Override
  public boolean evaluateParameters() {
    return false;
  }
}
