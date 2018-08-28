/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.RID;
import com.arcadedb.graph.VertexInternal;

public class DatabaseAsyncCreateOutEdge implements DatabaseAsyncTask {
  public final VertexInternal sourceVertex;
  public final RID            edgeRID;
  public final RID            destinationVertexRID;

  public DatabaseAsyncCreateOutEdge(final VertexInternal sourceVertex, final RID edgeRID, final RID destinationVertexRID) {
    this.sourceVertex = sourceVertex;
    this.edgeRID = edgeRID;
    this.destinationVertexRID = destinationVertexRID;
  }

  @Override
  public String toString() {
    return "CreateOutEdge(" + sourceVertex.getIdentity() + "->" + destinationVertexRID + ")";
  }
}
