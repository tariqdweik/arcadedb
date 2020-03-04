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
public class SQLFunctionInV extends SQLFunctionMove {
  public static final String NAME = "inV";

  public SQLFunctionInV() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final Database graph, final Identifiable iRecord, final String[] iLabels) {
    return e2v(graph, iRecord, Vertex.DIRECTION.IN, iLabels);
  }
}
