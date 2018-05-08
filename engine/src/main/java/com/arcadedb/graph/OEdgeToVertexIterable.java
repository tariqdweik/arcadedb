package com.arcadedb.graph;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class OEdgeToVertexIterable implements Iterable<PVertex> {
  private final Iterable<PEdge>   edges;
  private final PVertex.DIRECTION direction;

  public OEdgeToVertexIterable(Iterable<PEdge> edges, PVertex.DIRECTION direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<PVertex> iterator() {
    return new OEdgeToVertexIterator(edges.iterator(), direction);
  }
}
