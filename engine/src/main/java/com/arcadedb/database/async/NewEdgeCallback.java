/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.database.async;

import com.arcadedb.graph.Edge;

public interface NewEdgeCallback {
  void call(Edge newEdge, boolean createdSourceVertex, boolean createdDestinationVertex);
}
