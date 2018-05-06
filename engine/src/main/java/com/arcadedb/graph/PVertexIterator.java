package com.arcadedb.graph;

import com.arcadedb.database.PRID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class PVertexIterator implements Iterator<PVertex>, Iterable<PVertex> {
  private       PEdgeChunk    currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(PModifiableEdgeChunk.CONTENT_START_POSITION);

  public PVertexIterator(final PEdgeChunk current) {
    this.currentContainer = current;
  }

  @Override
  public boolean hasNext() {
    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(PModifiableEdgeChunk.CONTENT_START_POSITION);
      return true;
    }
    return false;
  }

  @Override
  public PVertex next() {
    if (!hasNext())
      throw new NoSuchElementException();

    currentContainer.getEdge(currentPosition); // SKIP EDGE
    final PRID rid = currentContainer.getVertex(currentPosition);

    return (PVertex) rid.getRecord();
  }

  @Override
  public Iterator<PVertex> iterator() {
    return this;
  }
}
