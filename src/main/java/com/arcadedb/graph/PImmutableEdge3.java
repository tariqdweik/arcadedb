package com.arcadedb.graph;

import com.arcadedb.database.PIdentifiable;

public interface PImmutableEdge3 {
  PIdentifiable getSourceVertex();

  PVertex.DIRECTION getDirection();

  String getTypeName();

  PIdentifiable getTargetVertex();

  PIdentifiable getEdge();
}
