package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;

public interface OSQLFunction {
  Object execute(Object targetObjects, PIdentifiable current, Object o, Object[] objects, OCommandContext ctx) ;

  Object getResult();

  void config(Object[] objects);

  boolean aggregateResults();
}
