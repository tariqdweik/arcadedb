package com.arcadedb.graph;

import com.arcadedb.database.PRID;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class PEdgeIterator implements Iterator<PEdge>, Iterable<PEdge> {
  private       PEdgeChunk    currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(PModifiableEdgeChunk.CONTENT_START_POSITION);

  public PEdgeIterator(final PEdgeChunk current) {
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
  public PEdge next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final PRID rid = currentContainer.getEdge(currentPosition);
    currentContainer.getVertex(currentPosition); // SKIP VERTEX

    return (PEdge) rid.getRecord();
  }

  @Override
  public Iterator<PEdge> iterator() {
    return this;
  }
}
