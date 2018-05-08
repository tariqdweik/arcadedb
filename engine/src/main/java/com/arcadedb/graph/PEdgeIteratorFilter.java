package com.arcadedb.graph;

import com.arcadedb.database.PDatabaseInternal;

import java.util.NoSuchElementException;

public class PEdgeIteratorFilter extends PIteratorFilterBase<PEdge> {
  public PEdgeIteratorFilter(final PDatabaseInternal database, final PEdgeChunk current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(true);
  }

  @Override
  public PEdge next() {
    if (next == null)
      throw new NoSuchElementException();

    try {
      return (PEdge) next.getRecord();
    } finally {
      next = null;
    }
  }
}
