/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class EdgeToVertexIterator implements Iterator<Vertex> {
  private final Iterator<Edge>   edgeIterator;
  private final Vertex.DIRECTION direction;

  public EdgeToVertexIterator(Iterator<Edge> iterator, Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.BOTH) {
      throw new IllegalArgumentException("edge to vertex iterator does not support BOTH as direction");
    }
    this.edgeIterator = iterator;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    return edgeIterator.hasNext();
  }

  @Override
  public Vertex next() {
    return edgeIterator.next().getVertex(direction);
  }
}
