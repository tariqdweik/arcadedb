/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.utility.Callable;

/**
 * Remove the first occurrence of elements from a collection.
 * 
 * @see SQLMethodRemoveAll
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodRemove extends OAbstractSQLMethod {

  public static final String NAME = "remove";

  public SQLMethodRemove() {
    super(NAME, 1, -1);
  }

  @Override
  public Object execute( Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, Object ioResult,
      Object[] iParams) {
    if (iParams != null && iParams.length > 0 && iParams[0] != null) {
      iParams = MultiValue.array(iParams, Object.class, new Callable<Object, Object>() {

        @Override
        public Object call(final Object iArgument) {
          if (iArgument instanceof String && ((String) iArgument).startsWith("$")) {
            return iContext.getVariable((String) iArgument);
          }
          return iArgument;
        }
      });
      for (Object o : iParams) {
        ioResult = MultiValue.remove(ioResult, o, false);
      }
    }

    return ioResult;
  }

  @Override
  public String getSyntax() {
    return "remove(<item>*)";
  }
}
