/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;

/**
 * A Vertex represents the main information in a Property Graph. Vertices are connected with edges. Vertices can be Immutable (read-only) and Mutable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 * @see Edge
 */
public interface Vertex extends Document {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  MutableVertex modify();

  MutableEdge newEdge(String edgeType, Identifiable toVertex, boolean bidirectional, final Object... properties);

  ImmutableLightEdge newLightEdge(String edgeType, Identifiable toVertex, boolean bidirectional);

  long countEdges(DIRECTION direction, String edgeType);

  Iterable<Edge> getEdges();

  Iterable<Edge> getEdges(DIRECTION direction, String... edgeTypes);

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
  Iterable<Vertex> getVertices(DIRECTION direction, String... edgeTypes);

  boolean isConnectedTo(Identifiable toVertex);

  boolean isConnectedTo(Identifiable toVertex, DIRECTION direction);
}
