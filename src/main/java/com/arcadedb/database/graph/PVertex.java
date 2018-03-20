package com.arcadedb.database.graph;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;
import com.arcadedb.engine.PGraphCursorEntry;

import java.util.Iterator;

public interface PVertex extends PRecord {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  void newEdge(String edgeType, PIdentifiable toVertex, boolean bidirectional);

  Iterator<PEdge> getEdges(DIRECTION direction, String edgeType);

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
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterator<PGraphCursorEntry> getConnectedVertices(DIRECTION direction);

  Iterator<PGraphCursorEntry> getConnectedVertices(DIRECTION direction, String edgeType);

  boolean isConnectedTo(PIdentifiable toVertex);

  boolean isConnectedTo(PIdentifiable toVertex, DIRECTION direction);

  boolean isConnectedTo(PIdentifiable toVertex, DIRECTION direction, String edgeType);
}
