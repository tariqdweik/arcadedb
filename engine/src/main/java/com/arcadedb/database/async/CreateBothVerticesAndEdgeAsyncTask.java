/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;

/**
 * Asynchronous Task that creates both vertices and the edge that connects them.
 */
public class CreateBothVerticesAndEdgeAsyncTask extends CreateEdgeAsyncTask {
  private final String   sourceVertexType;
  private final String[] sourceVertexAttributeNames;
  private final Object[] sourceVertexAttributeValues;

  private final String   destinationVertexType;
  private final String[] destinationVertexAttributeNames;
  private final Object[] destinationVertexAttributeValues;

  public CreateBothVerticesAndEdgeAsyncTask(final String sourceVertexType, final String[] sourceVertexAttributeNames,
      final Object[] sourceVertexAttributeValues, final String destinationVertexType, final String[] destinationVertexAttributeNames,
      final Object[] destinationVertexAttributeValues, final String edgeType, final Object[] edgeAttributes, final boolean bidirectional, final boolean light,
      final NewEdgeCallback callback) {
    super(null, null, edgeType, edgeAttributes, bidirectional, light, callback);

    this.sourceVertexType = sourceVertexType;
    this.sourceVertexAttributeNames = sourceVertexAttributeNames;
    this.sourceVertexAttributeValues = sourceVertexAttributeValues;

    this.destinationVertexType = destinationVertexType;
    this.destinationVertexAttributeNames = destinationVertexAttributeNames;
    this.destinationVertexAttributeValues = destinationVertexAttributeValues;
  }

  public void execute(final DatabaseInternal database) {
    final MutableVertex sourceVertex = database.newVertex(sourceVertexType);
    for (int i = 0; i < sourceVertexAttributeNames.length; ++i)
      sourceVertex.set(sourceVertexAttributeNames[i], sourceVertexAttributeValues[i]);
    sourceVertex.save();

    final MutableVertex destinationVertex = database.newVertex(destinationVertexType);
    for (int i = 0; i < destinationVertexAttributeNames.length; ++i)
      destinationVertex.set(destinationVertexAttributeNames[i], destinationVertexAttributeValues[i]);
    destinationVertex.save();

    createEdge(database, sourceVertex, destinationVertex, true, true);
  }

  @Override
  public String toString() {
    return "CreateBothVerticesAndEdgeAsyncTask(" + sourceVertexType + "->" + destinationVertexType + ")";
  }
}
