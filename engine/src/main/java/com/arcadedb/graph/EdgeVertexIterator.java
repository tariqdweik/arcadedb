/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;
import com.arcadedb.utility.Pair;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeVertexIterator implements Iterator<Pair<RID, RID>>, Iterable<Pair<RID, RID>> {
  private       EdgeSegment   currentContainer;
  private final AtomicInteger currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  public EdgeVertexIterator(final EdgeSegment current) {
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
  public Pair<RID, RID> next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final RID rid = currentContainer.getRID(currentPosition);
    final RID vertex = currentContainer.getRID(currentPosition);

    return new Pair(rid, vertex);
  }

  @Override
  public Iterator<Pair<RID, RID>> iterator() {
    return this;
  }
}
