/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeIterator implements Iterator<Edge>, Iterable<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;
  private       EdgeSegment      currentContainer;
  private final AtomicInteger    currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  public EdgeIterator(final EdgeSegment current, final RID vertex, final Vertex.DIRECTION direction) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");

    this.currentContainer = current;
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    if (currentContainer == null)
      return false;

    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
      return currentPosition.get() < currentContainer.getUsed();
    }
    return false;
  }

  @Override
  public Edge next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final RID nextEdgeRID = currentContainer.getRID(currentPosition);
    final RID nextVertexRID = currentContainer.getRID(currentPosition); // SKIP VERTEX

    if (nextEdgeRID.getPosition() < 0) {
      // CREATE LIGHTWEIGHT EDGE

      final String edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdgeRID.getBucketId()).getName();

      if (direction == Vertex.DIRECTION.OUT)
        return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, vertex, nextVertexRID);
      else
        return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, nextVertexRID, vertex);
    }

    return nextEdgeRID.getEdge();
  }

  @Override
  public Iterator<Edge> iterator() {
    return this;
  }
}
