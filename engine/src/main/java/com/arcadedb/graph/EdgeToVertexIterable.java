/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class EdgeToVertexIterable implements Iterable<Vertex> {
  private final Iterable<Edge>   edges;
  private final Vertex.DIRECTION direction;

  public EdgeToVertexIterable(Iterable<Edge> edges, Vertex.DIRECTION direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<Vertex> iterator() {
    return new EdgeToVertexIterator(edges.iterator(), direction);
  }
}
