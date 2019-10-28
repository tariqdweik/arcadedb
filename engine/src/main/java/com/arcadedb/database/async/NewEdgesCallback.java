/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.database.async;

import com.arcadedb.graph.Edge;

import java.util.List;

public interface NewEdgesCallback {
  void call(List<Edge> newEdges);
}
