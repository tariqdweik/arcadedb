package com.arcadedb.database.async;

import com.arcadedb.database.PRID;
import com.arcadedb.graph.PVertexInternal;

public class PDatabaseAsyncCreateOutEdge extends PDatabaseAsyncCommand {
  public final PVertexInternal sourceVertex;
  public final PRID            edgeRID;
  public final PRID            destinationVertexRID;

  public PDatabaseAsyncCreateOutEdge(final PVertexInternal sourceVertex, final PRID edgeRID, final PRID destinationVertexRID) {
    this.sourceVertex = sourceVertex;
    this.edgeRID = edgeRID;
    this.destinationVertexRID = destinationVertexRID;
  }
}
