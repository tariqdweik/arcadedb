/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeIterator implements Iterator<Edge>, Iterable<Edge> {
  private       EdgeChunk     currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(ModifiableEdgeChunk.CONTENT_START_POSITION);

  public EdgeIterator(final EdgeChunk current) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");

    this.currentContainer = current;
  }

  @Override
  public boolean hasNext() {
    if( currentContainer == null )
      return false;

    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(ModifiableEdgeChunk.CONTENT_START_POSITION);
      return currentPosition.get() < currentContainer.getUsed();
    }
    return false;
  }

  @Override
  public Edge next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final RID rid = currentContainer.getEdge(currentPosition);
    currentContainer.getVertex(currentPosition); // SKIP VERTEX

    return (Edge) rid.getRecord();
  }

  @Override
  public Iterator<Edge> iterator() {
    return this;
  }
}
