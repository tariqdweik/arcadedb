/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.VertexInternal;

/**
 * Asynchronous Task that creates the edge that connects 2 vertices.
 */
public class CreateEdgeAsyncTask extends DatabaseAsyncAbstractTask {
  protected final Identifiable sourceVertex;
  protected final Identifiable destinationVertex;

  protected final String   edgeType;
  protected final Object[] edgeAttributes;

  protected final boolean         bidirectional;
  protected final NewEdgeCallback callback;

  public CreateEdgeAsyncTask(final Identifiable sourceVertex, final Identifiable destinationVertex, final String edgeType,
      final Object[] edgeAttributes, final boolean bidirectional, final NewEdgeCallback callback) {
    this.sourceVertex = sourceVertex;
    this.destinationVertex = destinationVertex;

    this.edgeType = edgeType;
    this.edgeAttributes = edgeAttributes;

    this.bidirectional = bidirectional;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    createEdge(database, sourceVertex, destinationVertex, false, false);
  }

  protected void createEdge(final DatabaseInternal database, final Identifiable sourceVertex, final Identifiable destinationVertex,
      final boolean createdSourceVertex, boolean createdDestinationVertex) {

    final MutableEdge edge = database.getGraphEngine()
        .newEdge((VertexInternal) sourceVertex.getRecord(), edgeType, destinationVertex.getIdentity(), bidirectional, edgeAttributes);

    if (callback != null)
      callback.call(edge, createdSourceVertex, createdDestinationVertex);
  }

  @Override
  public String toString() {
    return "CreateEdgeAsyncTask(" + sourceVertex + "->" + destinationVertex + ")";
  }
}
