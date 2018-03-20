package com.arcadedb.engine;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.graph.PVertex;

/**
 * Immutable entry of index cursor.
 */
public class PGraphLSMCursorEntry implements PGraphCursorEntry {
  private final PIdentifiable     vertex;
  private final PVertex.DIRECTION direction;
  private final String            edgeTypeName;
  private final PIdentifiable     connectedVertex;
  private final PIdentifiable     edge;

  public PGraphLSMCursorEntry(final PIdentifiable vertex, final PVertex.DIRECTION direction, final String edgeTypeName,
      final PIdentifiable connectedVertex, final PIdentifiable edge) {
    this.vertex = vertex;
    this.direction = direction;
    this.edgeTypeName = edgeTypeName;
    this.connectedVertex = connectedVertex;
    this.edge = edge;
  }

  @Override
  public PIdentifiable getVertex() {
    return vertex;
  }

  @Override
  public PVertex.DIRECTION getDirection() {
    return direction;
  }

  @Override
  public String getEdgeTypeName() {
    return edgeTypeName;
  }

  @Override
  public PIdentifiable getConnectedVertex() {
    return connectedVertex;
  }

  @Override
  public PIdentifiable getEdge() {
    return edge;
  }
}
