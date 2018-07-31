/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;

public interface Vertex extends Document {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  ModifiableEdge newEdge(String edgeType, Identifiable toVertex, boolean bidirectional, final Object... properties);

  long countEdges(DIRECTION direction, String edgeType);

  Iterable<Edge> getEdges();

  Iterable<Edge> getEdges(DIRECTION direction);

  Iterable<Edge> getEdges(DIRECTION direction, String edgeTypes[]);

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterable<Vertex> getVertices();

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterable<Vertex> getVertices(DIRECTION direction);

  Iterable<Vertex> getVertices(DIRECTION direction, String edgeTypes[]);

  boolean isConnectedTo(Identifiable toVertex);

  boolean isConnectedTo(Identifiable toVertex, DIRECTION direction);
}
