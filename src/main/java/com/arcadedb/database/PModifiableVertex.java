package com.arcadedb.database;

import com.arcadedb.engine.PGraphCursorEntry;
import com.arcadedb.engine.PIndex;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PSchemaImpl;

import java.util.Iterator;

import static com.arcadedb.database.PGraph.NULL_RID;

public class PModifiableVertex extends PModifiableDocument implements PVertex {
  public PModifiableVertex(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PModifiableVertex(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public byte getRecordType() {
    return PVertex.RECORD_TYPE;
  }

  public void newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PEdgeType type = PGraph.getEdgeType(this, edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final Object[] outKeys = new Object[] { getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(),
        toVertex };

    // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
    edgeIndex.put(outKeys, NULL_RID);

    if (bidirectional) {
      final Object[] inKeys = new Object[] { toVertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId(),
          getIdentity() };

      // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
      edgeIndex.put(inKeys, NULL_RID);
    }
  }

  @Override
  public Iterator<PEdge> getEdges(final DIRECTION direction, final String edgeType) {
    return PGraph.getVertexEdges(this, direction, edgeType);
  }

  @Override
  public Iterator<PGraphCursorEntry> getConnectedVertices() {
    return PGraph.getVertexConnectedVertices(this);
  }

  @Override
  public Iterator<PGraphCursorEntry> getConnectedVertices(final DIRECTION direction, final String edgeType) {
    return PGraph.getVertexConnectedVertices(this, direction, edgeType);
  }

  @Override
  public boolean isConnectedTo(final PIdentifiable toVertex) {
    return PGraph.isVertexConnectedTo(this, toVertex);
  }

  @Override
  public boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction) {
    return PGraph.isVertexConnectedTo(this, toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction, final String edgeType) {
    return PGraph.isVertexConnectedTo(this, toVertex, direction, edgeType);
  }
}
