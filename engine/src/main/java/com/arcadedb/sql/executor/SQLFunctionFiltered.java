/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Identifiable;

public interface SQLFunctionFiltered {
  Object execute(Object targetObjects, Identifiable current, Object o, Object[] objects, Iterable<Identifiable> iPossibleResults,
      CommandContext ctx);
}
