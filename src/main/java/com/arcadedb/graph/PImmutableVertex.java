package com.arcadedb.graph;

import com.arcadedb.database.*;

public class PImmutableVertex extends PImmutableDocument implements PVertex {

  public PImmutableVertex(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public byte getRecordType() {
    return PVertex.RECORD_TYPE;
  }

  public PModifiableVertex modify() {
    return new PModifiableVertex(database, typeName, rid, buffer);
  }

  public PEdge newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    return PGraph.newEdge(this, edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public PCursor<PEdge> getEdges() {
    return PGraph.getEdges(this);
  }

  @Override
  public PCursor<PEdge> getEdges(final DIRECTION direction) {
    return PGraph.getEdges(this, direction);
  }

  @Override
  public PCursor<PEdge> getEdges(final DIRECTION direction, final String edgeType) {
    return PGraph.getEdges(this, direction, edgeType);
  }

  @Override
  public PCursor<PVertex> getVertices() {
    return PGraph.getVertices(this);
  }

  @Override
  public PCursor<PVertex> getVertices(final DIRECTION direction) {
    return PGraph.getVertices(this, direction);
  }

  @Override
  public PCursor<PVertex> getVertices(final DIRECTION direction, final String edgeType) {
    return PGraph.getVertices(this, direction, edgeType);
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
