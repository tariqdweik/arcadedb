/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Vertex;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public class SQLFunctionBoth extends SQLFunctionMove {
  public static final String NAME = "both";

  public SQLFunctionBoth() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final Database graph, final Identifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, Vertex.DIRECTION.BOTH, iLabels);
  }
}
