package com.arcadedb.graph;

import com.arcadedb.database.PIdentifiable;

public class PImmutableEdge2 implements PImmutableEdge3 {
  private final PIdentifiable     vertex;
  private final PVertex.DIRECTION direction;
  private final String            edgeTypeName;
  private final PIdentifiable     connectedVertex;
  private final PIdentifiable     edge;

  public PImmutableEdge2(final PIdentifiable vertex, final PVertex.DIRECTION direction, final String edgeTypeName,
      final PIdentifiable connectedVertex, final PIdentifiable edge) {
    this.vertex = vertex;
    this.direction = direction;
    this.edgeTypeName = edgeTypeName;
    this.connectedVertex = connectedVertex;
    this.edge = edge;
  }

  @Override
  public PIdentifiable getSourceVertex() {
    return vertex;
  }

  @Override
  public PVertex.DIRECTION getDirection() {
    return direction;
  }

  @Override
  public String getTypeName() {
    return edgeTypeName;
  }

  @Override
  public PIdentifiable getTargetVertex() {
    return connectedVertex;
  }

  @Override
  public PIdentifiable getEdge() {
    return edge;
  }
}
