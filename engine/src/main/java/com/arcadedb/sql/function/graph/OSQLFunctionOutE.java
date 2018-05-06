package com.arcadedb.sql.function.graph;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.graph.PVertex;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public class OSQLFunctionOutE extends OSQLFunctionMove {
  public static final String NAME = "outE";

  public OSQLFunctionOutE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final PDatabase graph, final PIdentifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, PVertex.DIRECTION.OUT, iLabels);
  }
}
