/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;

import java.util.NoSuchElementException;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  public EdgeIteratorFilter(final DatabaseInternal database, final EdgeChunk current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
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
      return next.getEdge();
    } finally {
      next = null;
    }
  }
}
