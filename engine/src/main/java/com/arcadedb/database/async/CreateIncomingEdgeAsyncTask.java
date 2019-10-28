/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;

/**
 * Asynchronous Task that creates the relationship between the destinationVertex and the sourceVertex as incoming.
 */
public class CreateIncomingEdgeAsyncTask extends DatabaseAsyncAbstractTask {
  protected final RID          sourceVertexRID;
  protected final Identifiable destinationVertex;
  protected final Identifiable edge;

  protected final NewEdgeCallback callback;

  public CreateIncomingEdgeAsyncTask(final RID sourceVertex, final Identifiable destinationVertex, final Identifiable edge,
      final NewEdgeCallback callback) {
    this.sourceVertexRID = sourceVertex;
    this.destinationVertex = destinationVertex;
    this.edge = edge;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    database.getGraphEngine().connectIncomingEdge(database, destinationVertex, sourceVertexRID, edge.getIdentity());

    if (callback != null)
      callback.call((Edge) edge.getRecord(), false, false);
  }

  @Override
  public String toString() {
    return "CreateIncomingEdgeAsyncTask(" + sourceVertexRID + "<-" + destinationVertex + ")";
  }
}
