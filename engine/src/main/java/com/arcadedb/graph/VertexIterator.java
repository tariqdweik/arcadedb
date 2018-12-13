/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexIterator implements Iterator<Vertex>, Iterable<Vertex> {
  private       EdgeSegment   currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  public VertexIterator(final EdgeSegment current) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");
    this.currentContainer = current;
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
  public Vertex next() {
    if (!hasNext())
      throw new NoSuchElementException();

    currentContainer.getRID(currentPosition); // SKIP EDGE
    final RID rid = currentContainer.getRID(currentPosition);

    return rid.getVertex();
  }

  @Override
  public Iterator<Vertex> iterator() {
    return this;
  }
}
