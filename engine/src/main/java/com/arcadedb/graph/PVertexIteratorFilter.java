package com.arcadedb.graph;

import com.arcadedb.database.PDatabaseInternal;

import java.util.NoSuchElementException;

public class PVertexIteratorFilter extends PIteratorFilterBase<PVertex> {
  public PVertexIteratorFilter(final PDatabaseInternal database, final PEdgeChunk current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(false);
  }

  @Override
  public PVertex next() {
    if (next == null)
      throw new NoSuchElementException();

    try {
      return (PVertex) next.getRecord();
    } finally {
      next = null;
    }
  }
}
