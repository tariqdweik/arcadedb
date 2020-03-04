/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transforms current value into a Map.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsMap extends OAbstractSQLMethod {

  public static final String NAME = "asmap";

  public SQLMethodAsMap() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (ioResult instanceof Map)
      // ALREADY A MAP
      return ioResult;

    if (ioResult == null) {
      // NULL VALUE, RETURN AN EMPTY MAP
      return Collections.EMPTY_MAP;
    }

    if (ioResult instanceof Document) {
      // CONVERT ODOCUMENT TO MAP
      return ((Document) ioResult).toMap();
    }

    Iterator<Object> iter;
    if (ioResult instanceof Iterator<?>) {
      iter = (Iterator<Object>) ioResult;
    } else if (ioResult instanceof Iterable<?>) {
      iter = ((Iterable<Object>) ioResult).iterator();
    } else {
      return null;
    }

    final HashMap<Object, Object> map = new HashMap<Object, Object>();
    while (iter.hasNext()) {
      final Object key = iter.next();
      if (iter.hasNext()) {
        final Object value = iter.next();
        map.put(key, value);
      }
    }

    return map;
  }
}
