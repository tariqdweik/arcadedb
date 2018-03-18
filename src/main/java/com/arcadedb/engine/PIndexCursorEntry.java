package com.arcadedb.engine;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PVertex;

public interface PIndexCursorEntry {
  PIdentifiable getVertex();

  PVertex.DIRECTION getDirection();

  String getEdgeTypeName();

  PIdentifiable getConnectedVertex();

  PIdentifiable getEdge();
}
