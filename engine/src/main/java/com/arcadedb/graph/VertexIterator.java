/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexIterator implements Iterator<Vertex>, Iterable<Vertex> {
  private       EdgeChunk     currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(ModifiableEdgeChunk.CONTENT_START_POSITION);

  public VertexIterator(final EdgeChunk current) {
    this.currentContainer = current;
  }

  @Override
  public boolean hasNext() {
    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(ModifiableEdgeChunk.CONTENT_START_POSITION);
      return true;
    }
    return false;
  }

  @Override
  public Vertex next() {
    if (!hasNext())
      throw new NoSuchElementException();

    currentContainer.getEdge(currentPosition); // SKIP EDGE
    final RID rid = currentContainer.getVertex(currentPosition);

    return (Vertex) rid.getRecord();
  }

  @Override
  public Iterator<Vertex> iterator() {
    return this;
  }
}
