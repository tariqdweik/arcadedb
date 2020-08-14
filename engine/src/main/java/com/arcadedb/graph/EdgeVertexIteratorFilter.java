/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.utility.Pair;

import java.util.NoSuchElementException;

public class EdgeVertexIteratorFilter extends IteratorFilterBase<Pair<RID, RID>> {
  public EdgeVertexIteratorFilter(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(false);
  }

  @Override
  public Pair<RID, RID> next() {
    hasNext();

    if (next == null)
      throw new NoSuchElementException();

    try {
      return new Pair(nextEdge, next);
    } finally {
      next = null;
    }
  }
}
