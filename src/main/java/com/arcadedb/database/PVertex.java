package com.arcadedb.database;

import com.arcadedb.engine.PGraphCursorEntry;

import java.util.Iterator;

public interface PVertex extends PRecord {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  Iterator<PEdge> getEdges(final DIRECTION direction, final String edgeType);

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterator<PGraphCursorEntry> getConnectedVertices();

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeType  Edge type name to filter
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterator<PGraphCursorEntry> getConnectedVertices(final DIRECTION direction, final String edgeType);

  boolean isConnectedTo(final PIdentifiable toVertex);

  boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction);

  boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction, final String edgeType);
}
