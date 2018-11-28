/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;

/**
 * Asynchronous Task that creates the destination vertex and the edge that connects it to the existent source vertex.
 */
public class CreateDestinationVertexAndEdgeAsyncTask extends CreateEdgeAsyncTask {
  private final String   destinationVertexType;
  private final String[] destinationVertexAttributeNames;
  private final Object[] destinationVertexAttributeValues;

  public CreateDestinationVertexAndEdgeAsyncTask(final RID sourceVertex, final String destinationVertexType,
      final String[] destinationVertexAttributeNames, final Object[] destinationVertexAttributeValues, final String edgeType,
      final Object[] edgeAttributes, final boolean bidirectional, final NewEdgeCallback callback) {
    super(sourceVertex, null, edgeType, edgeAttributes, bidirectional, callback);

    this.destinationVertexType = destinationVertexType;
    this.destinationVertexAttributeNames = destinationVertexAttributeNames;
    this.destinationVertexAttributeValues = destinationVertexAttributeValues;
  }

  public void execute(final DatabaseInternal database) {
    final MutableVertex destinationVertex = database.newVertex(destinationVertexType);
    for (int i = 0; i < destinationVertexAttributeNames.length; ++i)
      destinationVertex.set(destinationVertexAttributeNames[i], destinationVertexAttributeValues[i]);
    destinationVertex.save();

    createEdge(database, sourceVertex, destinationVertex, false, true);
  }

  @Override
  public String toString() {
    return "CreateDestinationVertexAndEdgeAsyncTask(" + sourceVertex + "->" + destinationVertexType + ")";
  }
}
