package com.arcadedb.graph;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;

import java.util.Iterator;

public interface PVertex extends PDocument {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  PEdge newEdge(String edgeType, PIdentifiable toVertex, boolean bidirectional, final Object... properties);

  long countEdges(DIRECTION direction, String edgeType);

  Iterator<PEdge> getEdges();

  Iterator<PEdge> getEdges(DIRECTION direction);

  Iterator<PEdge> getEdges(DIRECTION direction, String edgeType);

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterator<PVertex> getVertices();

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterator<PVertex> getVertices(DIRECTION direction);

  Iterator<PVertex> getVertices(DIRECTION direction, String edgeType);

  boolean isConnectedTo(PIdentifiable toVertex);

  boolean isConnectedTo(PIdentifiable toVertex, DIRECTION direction);
}
