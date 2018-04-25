package com.arcadedb.graph;

import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.PBucket;
import com.arcadedb.schema.PDocumentType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PEdgeLinkedList {
  private final PVertex           vertex;
  private final PVertex.DIRECTION direction;
  private       PEdgeChunk        first;

  public PEdgeLinkedList(final PVertex vertex, final PVertex.DIRECTION direction, final PEdgeChunk first) {
    this.vertex = vertex;
    this.direction = direction;
    this.first = first;
  }

  public PEdgeIterator edgeIterator() {
    return new PEdgeIterator(first);
  }

  public PEdgeIteratorFilter edgeIterator(final String edgeType) {
    return new PEdgeIteratorFilter((PDatabaseInternal) vertex.getDatabase(), first, edgeType);
  }

  public PVertexIterator vertexIterator() {
    return new PVertexIterator(first);
  }

  public PVertexIteratorFilter vertexIterator(final String edgeType) {
    return new PVertexIteratorFilter((PDatabaseInternal) vertex.getDatabase(), first, edgeType);
  }

  public boolean containsEdge(final PRID rid) {
    PEdgeChunk current = first;
    while (current != null) {
      if (current.containsEdge(rid))
        return true;

      current = current.getNext();
    }

    return false;
  }

  public boolean containsVertex(final PRID rid) {
    PEdgeChunk current = first;
    while (current != null) {
      if (current.containsVertex(rid))
        return true;

      current = current.getNext();
    }

    return false;
  }

  /**
   * Counts the items in the linked list.
   *
   * @param edgeType Type of edge to filter for the counting. If it is null, any type is counted.
   *
   * @return
   */
  public long count(final String edgeType) {
    long total = 0;

    final Set<Integer> fileIdToFilter;
    if (edgeType != null) {
      final PDocumentType type = vertex.getDatabase().getSchema().getType(edgeType);
      final List<PBucket> buckets = type.getBuckets();
      fileIdToFilter = new HashSet<Integer>(buckets.size());
      for (PBucket b : buckets)
        fileIdToFilter.add(b.getId());
    } else
      fileIdToFilter = null;

    PEdgeChunk current = first;
    while (current != null) {
      total += current.count(fileIdToFilter);
      current = current.getNext();
    }

    return total;
  }

  public void add(final PRID edgeRID, final PRID vertexRID) {
    if (first.add(edgeRID, vertexRID))
      ((PDatabaseInternal) vertex.getDatabase()).updateRecord(first);
    else {
      // CHUNK FULL, ALLOCATE A NEW ONE
      PDatabaseInternal database = (PDatabaseInternal) vertex.getDatabase();

      final PModifiableEdgeChunk newChunk = new PModifiableEdgeChunk(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setNext(first);

      database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

      final PModifiableVertex modifiableV = (PModifiableVertex) vertex.modify();

      if (direction == PVertex.DIRECTION.OUT)
        modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
      else
        modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

      first = newChunk;

      modifiableV.save();
    }
  }

  public void removeEdge(final PRID edgeRID) {
    PEdgeChunk current = first;
    while (current != null) {
      current.removeEdge(edgeRID);
      current = current.getNext();
    }
  }

  public void removeVertex(final PRID vertexRID) {
    PEdgeChunk current = first;
    while (current != null) {
      current.removeVertex(vertexRID);
      current = current.getNext();
    }
  }

  private int computeBestSize() {
    int currentSize = first.getRecordSize();
    if (currentSize < 8192)
      currentSize *= 2;

    if (currentSize > 8192)
      currentSize = 8192;

    return currentSize;
  }
}
