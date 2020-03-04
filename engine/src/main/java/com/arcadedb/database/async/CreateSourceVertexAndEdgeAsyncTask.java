/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;

/**
 * Asynchronous Task that creates the source vertex and the edge that connects it to the existent destination vertex.
 */
public class CreateSourceVertexAndEdgeAsyncTask extends CreateEdgeAsyncTask {
  private final String   sourceVertexType;
  private final String[] sourceVertexAttributeNames;
  private final Object[] sourceVertexAttributeValues;

  public CreateSourceVertexAndEdgeAsyncTask(final String sourceVertexType, final String[] sourceVertexAttributeNames,
      final Object[] sourceVertexAttributeValues, final RID destinationVertex, final String edgeType, final Object[] edgeAttributes,
      final boolean bidirectional, final boolean light, final NewEdgeCallback callback) {
    super(null, destinationVertex, edgeType, edgeAttributes, bidirectional, light, callback);

    this.sourceVertexType = sourceVertexType;
    this.sourceVertexAttributeNames = sourceVertexAttributeNames;
    this.sourceVertexAttributeValues = sourceVertexAttributeValues;
  }

  public void execute(final DatabaseInternal database) {
    final MutableVertex sourceVertex = database.newVertex(sourceVertexType);
    for (int i = 0; i < sourceVertexAttributeNames.length; ++i)
      sourceVertex.set(sourceVertexAttributeNames[i], sourceVertexAttributeValues[i]);
    sourceVertex.save();

    createEdge(database, sourceVertex, destinationVertex, true, false);
  }

  @Override
  public String toString() {
    return "CreateSourceVertexAndEdgeAsyncTask(" + sourceVertexType + "->" + destinationVertex + ")";
  }
}
