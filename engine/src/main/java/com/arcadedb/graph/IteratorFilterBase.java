package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.PEdgeType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IteratorFilterBase<T> implements Iterator<T>, Iterable<T> {
  protected       EdgeChunk     currentContainer;
  protected final AtomicInteger currentPosition = new AtomicInteger(ModifiableEdgeChunk.CONTENT_START_POSITION);

  protected RID              next;
  protected HashSet<Integer> validBuckets;

  protected IteratorFilterBase(final DatabaseInternal database, final EdgeChunk current, final String[] edgeTypes) {
    this.currentContainer = current;

    for (String e : edgeTypes) {
      final PEdgeType type = (PEdgeType) database.getSchema().getType(e);

      final List<Bucket> buckets = type.getBuckets(true);
      validBuckets = new HashSet<>(buckets.size());
      for (Bucket b : buckets)
        validBuckets.add(b.getId());
    }
  }

  protected boolean hasNext(final boolean edge) {
    if (next != null)
      return true;

    RID nextEdge;
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
          currentPosition.set(ModifiableEdgeChunk.CONTENT_START_POSITION);
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
