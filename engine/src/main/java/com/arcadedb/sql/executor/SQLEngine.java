/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.function.DefaultSQLFunctionFactory;
import com.arcadedb.sql.parser.Statement;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.MultiIterator;

import java.util.Iterator;

public class SQLEngine {
  private static final SQLEngine                 INSTANCE = new SQLEngine();
  private final        DefaultSQLFunctionFactory functions;

  protected SQLEngine() {
    functions = new DefaultSQLFunctionFactory();
  }

  public static Object foreachRecord(final Callable<Object, Identifiable> iCallable, Object iCurrent,
      final CommandContext iContext) {
    if (iCurrent == null)
      return null;

    if (iCurrent instanceof Iterable) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (MultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final MultiIterator<Object> result = new MultiIterator<Object>();
      for (Object o : MultiValue.getMultiValueIterable(iCurrent, false)) {
        if (iContext != null && !iContext.checkTimeout())
          return null;

        if (MultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : MultiValue.getMultiValueIterable(o, false)) {
            result.add(iCallable.call((Identifiable) inner));
          }
        } else
          result.add(iCallable.call((Identifiable) o));
      }
      return result;
    } else if (iCurrent instanceof Identifiable) {
      return iCallable.call((Identifiable) iCurrent);
    } else if (iCurrent instanceof Result) {
      return iCallable.call(((Result) iCurrent).toElement());
    }

    return null;
  }

  public static SQLEngine getInstance() {
    return INSTANCE;
  }

  public SQLFunction getFunction(final String name) {
    return functions.createFunction(name);
  }

  public static SQLMethod getMethod(final String name) {
    return null;
  }

  public static Statement parse(final String query, final DatabaseInternal database) {
    return database.getStatementCache().get(query);
  }
}
