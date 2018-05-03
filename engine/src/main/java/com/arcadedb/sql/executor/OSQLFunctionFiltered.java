package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;

public interface OSQLFunctionFiltered {
  Object execute(Object targetObjects, PIdentifiable current, Object o, Object[] objects, Iterable<PIdentifiable> iPossibleResults,
      OCommandContext ctx);
}
