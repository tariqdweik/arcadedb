package com.arcadedb.graph;

import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.PBucket;
import com.arcadedb.schema.PEdgeType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PIteratorFilterBase<T> implements Iterator<T>, Iterable<T> {
  protected       PEdgeChunk    currentContainer;
  protected final AtomicInteger currentPosition = new AtomicInteger(PModifiableEdgeChunk.CONTENT_START_POSITION);

  protected PRID             next;
  protected HashSet<Integer> validBuckets;

  protected PIteratorFilterBase(final PDatabaseInternal database, final PEdgeChunk current, final String[] edgeTypes) {
    this.currentContainer = current;

    for (String e : edgeTypes) {
      final PEdgeType type = (PEdgeType) database.getSchema().getType(e);

      final List<PBucket> buckets = type.getBuckets(true);
      validBuckets = new HashSet<>(buckets.size());
      for (PBucket b : buckets)
        validBuckets.add(b.getId());
    }
  }

  protected boolean hasNext(final boolean edge) {
    if (next != null)
      return true;

    PRID nextEdge;
    while (true) {
      if (currentPosition.get() < currentContainer.getUsed()) {
        if (edge) {
          nextEdge = next = currentContainer.getEdge(currentPosition);
          currentContainer.getVertex(currentPosition); // SKIP VERTEX
        } else {
          nextEdge = currentContainer.getEdge(currentPosition);
          next = currentContainer.getVertex(currentPosition);
        }

        if (validBuckets.contains(nextEdge.getBucketId()))
          return true;
      } else {
        // FETCH NEXT CHUNK
        currentContainer = currentContainer.getNext();
        if (currentContainer != null) {
          currentPosition.set(PModifiableEdgeChunk.CONTENT_START_POSITION);
        } else
          // END
          break;
      }
    }

    return false;
  }

  @Override
  public Iterator<T> iterator() {
    return this;
  }
}
