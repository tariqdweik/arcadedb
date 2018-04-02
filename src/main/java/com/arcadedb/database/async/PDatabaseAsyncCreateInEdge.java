package com.arcadedb.database.async;

import com.arcadedb.database.PRID;
import com.arcadedb.graph.PVertexInternal;

public class PDatabaseAsyncCreateInEdge extends PDatabaseAsyncCommand {
  public final PVertexInternal destinationVertex;
  public final PRID            edgeRID;
  public final PRID            sourceVertexRID;

  public PDatabaseAsyncCreateInEdge(final PVertexInternal destinationVertex, final PRID edgeRID, final PRID sourceVertexRID) {
    this.destinationVertex = destinationVertex;
    this.edgeRID = edgeRID;
    this.sourceVertexRID = sourceVertexRID;
  }
}
