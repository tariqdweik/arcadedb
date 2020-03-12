/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.EdgeType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IteratorFilterBase<T> implements Iterator<T>, Iterable<T> {
  protected final DatabaseInternal database;
  protected       EdgeSegment      currentContainer;
  protected final AtomicInteger    currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);

  protected RID          nextEdge;
  protected RID          nextVertex;
  protected RID          next;
  protected Set<Integer> validBuckets;

  protected IteratorFilterBase(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    this.database = database;
    this.currentContainer = current;

    validBuckets = new HashSet<>();
    for (String e : edgeTypes) {
      if (!database.getSchema().existsType(e))
        continue;

      final EdgeType type = (EdgeType) database.getSchema().getType(e);

      final List<Bucket> buckets = type.getBuckets(true);
      for (Bucket b : buckets)
        validBuckets.add(b.getId());
    }
  }

  protected boolean hasNext(final boolean edge) {
    if (next != null)
      return true;

    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed()) {
        if (edge) {
          nextEdge = next = currentContainer.getRID(currentPosition);
          nextVertex = currentContainer.getRID(currentPosition); // SKIP VERTEX

        } else {
          nextEdge = currentContainer.getRID(currentPosition);
          nextVertex = next = currentContainer.getRID(currentPosition);
        }

        if (validBuckets.contains(nextEdge.getBucketId()))
          return true;

      } else {
        // FETCH NEXT CHUNK
        currentContainer = currentContainer.getNext();
        if (currentContainer != null) {
          currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
        } else
          // END
          break;
      }
    }

    return false;
  }

  @Override
  public void remove() {
    currentContainer.removeEntry(currentPosition.get());
  }

  public RID getNextVertex() {
    return nextVertex;
  }

  @Override
  public Iterator<T> iterator() {
    return this;
  }
}
