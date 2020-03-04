/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;

import java.util.NoSuchElementException;

public class VertexIteratorFilter extends IteratorFilterBase<Vertex> {
  public VertexIteratorFilter(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(false);
  }

  @Override
  public Vertex next() {
    if (next == null)
      throw new NoSuchElementException();

    try {
      return next.getVertex();
    } finally {
      next = null;
    }
  }
}
