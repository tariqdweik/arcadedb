/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.VertexInternal;

import java.util.List;

/**
 * Asynchronous Task that creates the relationship between the sourceVertex and the destinationVertex as outgoing.
 */
public class CreateOutgoingEdgesAsyncTask extends DatabaseAsyncAbstractTask {
  private final VertexInternal                        sourceVertex;
  private final List<GraphEngine.CreateEdgeOperation> connections;
  private final boolean                               edgeBidirectional;
  private final NewEdgesCallback                      callback;

  public CreateOutgoingEdgesAsyncTask(final VertexInternal sourceVertex, final List<GraphEngine.CreateEdgeOperation> connections,
      final boolean edgeBidirectional, final NewEdgesCallback callback) {
    this.sourceVertex = sourceVertex;
    this.connections = connections;
    this.edgeBidirectional = edgeBidirectional;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    final List<Edge> edges = database.getGraphEngine().newEdges(database, sourceVertex, connections, edgeBidirectional);

    if (callback != null)
      callback.call(edges);
  }

  @Override
  public String toString() {
    return "CreateOutgoingEdgesAsyncTask(" + sourceVertex + "->" + connections.size() + ")";
  }
}
