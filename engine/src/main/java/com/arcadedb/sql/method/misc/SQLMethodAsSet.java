/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.*;

/**
 * Transforms current value in a Set.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodAsSet extends OAbstractSQLMethod {

  public static final String NAME = "asset";

  public SQLMethodAsSet() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (ioResult instanceof Set)
      // ALREADY A SET
      return ioResult;

    if (ioResult == null)
      // NULL VALUE, RETURN AN EMPTY SET
      return Collections.EMPTY_SET;

    if (ioResult instanceof Collection<?>) {
      return new HashSet<>((Collection<Object>) ioResult);
    } else if (ioResult instanceof Iterable<?>) {
      ioResult = ((Iterable<?>) ioResult).iterator();
    }

    if (ioResult instanceof Iterator<?>) {
      final Set<Object> set = new HashSet<>();

      for (Iterator<Object> iter = (Iterator<Object>) ioResult; iter.hasNext(); )
        set.add(iter.next());

      return set;
    }

    // SINGLE ITEM: ADD IT AS UNIQUE ITEM
    return Collections.singleton(ioResult);
  }
}
