package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.PGraphCursorEntry;

import java.util.Iterator;

public class PImmutableVertex extends PImmutableDocument implements PVertex {

  public PImmutableVertex(PDatabase graph, String typeName, PRID rid, PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public byte getRecordType() {
    return PVertex.RECORD_TYPE;
  }

  public PModifiableVertex modify() {
    return new PModifiableVertex(database, typeName, rid, buffer);
  }

  public PEdge newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional) {
    return PGraph.newEdge(this, edgeType, toVertex, bidirectional);
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
  public Iterator<PGraphCursorEntry> getConnectedVertices(final DIRECTION direction) {
    return PGraph.getVertexConnectedVertices(this, direction);
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
