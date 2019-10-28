/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.*;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodKeys extends OAbstractSQLMethod {

  public static final String NAME = "keys";

  public SQLMethodKeys() {
    super(NAME);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (ioResult instanceof Map) {
      return ((Map<?, ?>) ioResult).keySet();
    }
    if (ioResult instanceof Document) {
      return Arrays.asList(((Document) ioResult).getPropertyNames());
    }
    if (ioResult instanceof Collection) {
      List result = new ArrayList();
      for (Object o : (Collection) ioResult) {
        result.addAll((Collection) execute(iThis, iCurrentRecord, iContext, o, iParams));
      }
      return result;
    }
    return null;
  }
}
