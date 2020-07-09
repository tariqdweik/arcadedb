/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;

  public EdgeIteratorFilter(final DatabaseInternal database, final Vertex vertex, final Vertex.DIRECTION direction, final EdgeSegment current,
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
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, vertex.getIdentity(), nextVertex);
        else
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, nextVertex, vertex.getIdentity());
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

  @Override
  protected void handleCorruption(final Exception e, final RID edge, final RID vertex) {
    if ((e instanceof RecordNotFoundException || e instanceof SchemaException) &&//
        database.getMode() == PaginatedFile.MODE.READ_WRITE) {

      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Fixing it...", e, edge, vertex != null ? "vertex " + vertex : "");

      database.transaction((tx) -> {
        final EdgeLinkedList outEdges = database.getGraphEngine().getEdgeHeadChunk((VertexInternal) this.vertex, direction);
        if (outEdges != null)
          outEdges.removeEdgeRID(edge);

      }, true);

    } else
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Skip it.", e, edge, vertex != null ? "vertex " + vertex : "");
  }
}
