/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;

  public EdgeIteratorFilter(final DatabaseInternal database, final RID vertex, final Vertex.DIRECTION direction, final EdgeSegment current,
      final String[] edgeTypes) {
    super(database, current, edgeTypes);
    this.direction = direction;
    this.vertex = vertex;
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
      if (next.getPosition() < 0) {
        // LIGHTWEIGHT EDGE
        final String edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdge.getBucketId()).getName();

        if (direction == Vertex.DIRECTION.OUT)
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, vertex, nextVertex);
        else
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, nextVertex, vertex);
      }

      return next.getEdge();
    } catch (RecordNotFoundException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);

      next = null;
      if (hasNext())
        return next();

      throw e;

    } catch (SchemaException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);
      throw e;
    } finally {
      next = null;
    }
  }
}
