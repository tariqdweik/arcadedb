package com.arcadedb.graph;

import com.arcadedb.database.PCursor;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;

public interface PVertex extends PRecord {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  PEdge newEdge(String edgeType, PIdentifiable toVertex, boolean bidirectional, final Object... properties);

  PCursor<PEdge> getEdges();

  PCursor<PEdge> getEdges(DIRECTION direction);

  PCursor<PEdge> getEdges(DIRECTION direction, String edgeType);

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  PCursor<PVertex> getVertices();

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  PCursor<PVertex> getVertices(DIRECTION direction);

  PCursor<PVertex> getVertices(DIRECTION direction, String edgeType);

  boolean isConnectedTo(PIdentifiable toVertex);

  boolean isConnectedTo(PIdentifiable toVertex, DIRECTION direction);

  boolean isConnectedTo(PIdentifiable toVertex, DIRECTION direction, String edgeType);
}
