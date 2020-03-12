/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;

import java.util.NoSuchElementException;
import java.util.logging.Level;

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
    } catch (SchemaException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading vertex %s from edge %s", e, next, nextEdge);
      throw e;
    } finally {
      next = null;
    }
  }
}
