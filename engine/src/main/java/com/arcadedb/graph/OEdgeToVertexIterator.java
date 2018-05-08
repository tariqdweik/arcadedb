package com.arcadedb.graph;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class OEdgeToVertexIterator implements Iterator<PVertex> {
  private final Iterator<PEdge>   edgeIterator;
  private final PVertex.DIRECTION direction;

  public OEdgeToVertexIterator(Iterator<PEdge> iterator, PVertex.DIRECTION direction) {
    if (direction == PVertex.DIRECTION.BOTH) {
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
  public PVertex next() {
    return edgeIterator.next().getVertex(direction);
  }
}
