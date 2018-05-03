package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;

public interface OSQLMethod {
  Object execute(Object targetObjects, PIdentifiable val, OCommandContext ctx, Object targetObjects1, Object[] objects);
}
