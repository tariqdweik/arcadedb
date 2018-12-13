/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

import java.util.NoSuchElementException;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;

  public EdgeIteratorFilter(final DatabaseInternal database, final RID vertex, final Vertex.DIRECTION direction, final EdgeSegment current,
      final String[] edgeTypes) {
    super(database, current, edgeTypes);
    this.direction = direction;
    this.vertex = vertex;
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(true);
  }

  @Override
  public Edge next() {
    if (next == null)
      throw new NoSuchElementException();

    try {
      if (next.getPosition() < 0) {
        // LIGHTWEIGHT EDGE
        final String edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdge.getBucketId()).getName();

        if (direction == Vertex.DIRECTION.OUT)
          return new ImmutableEdge(currentContainer.getDatabase(), edgeType, nextEdge, vertex, nextVertex);
        else
          return new ImmutableEdge(currentContainer.getDatabase(), edgeType, nextEdge, nextVertex, vertex);
      }

      return next.getEdge();
    } finally {
      next = null;
    }
  }
}
