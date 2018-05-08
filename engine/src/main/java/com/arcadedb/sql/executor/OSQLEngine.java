package com.arcadedb.sql.executor;

import com.arcadedb.database.PEmbeddedDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.function.ODefaultSQLFunctionFactory;
import com.arcadedb.sql.parser.OStatementCache;
import com.arcadedb.sql.parser.Statement;
import com.arcadedb.utility.PCallable;
import com.arcadedb.utility.PMultiIterator;

import java.util.Iterator;

public class OSQLEngine {
  private static final OSQLEngine                 INSTANCE = new OSQLEngine();
  private final        ODefaultSQLFunctionFactory functions;

  protected OSQLEngine() {
    functions = new ODefaultSQLFunctionFactory();
  }

  public static Object foreachRecord(final PCallable<Object, PIdentifiable> iCallable, Object iCurrent,
      final OCommandContext iContext) {
    if (iCurrent == null)
      return null;

    if (iCurrent instanceof Iterable) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (OMultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final PMultiIterator<Object> result = new PMultiIterator<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(iCurrent, false)) {
        if (iContext != null && !iContext.checkTimeout())
          return null;

        if (OMultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : OMultiValue.getMultiValueIterable(o, false)) {
            result.add(iCallable.call((PIdentifiable) inner));
          }
        } else
          result.add(iCallable.call((PIdentifiable) o));
      }
      return result;
    } else if (iCurrent instanceof PIdentifiable) {
      return iCallable.call((PIdentifiable) iCurrent);
    } else if (iCurrent instanceof OResult) {
      return iCallable.call(((OResult) iCurrent).toElement());
    }

    return null;
  }

  public static OSQLEngine getInstance() {
    return INSTANCE;
  }

  public OSQLFunction getFunction(String name) {
    return functions.createFunction(name);
  }

  public static OSQLMethod getMethod(String name) {
    return null;
  }

  public static Statement parse(String query, PEmbeddedDatabase pDatabase) {
    return OStatementCache.get(query, pDatabase);
  }
}
