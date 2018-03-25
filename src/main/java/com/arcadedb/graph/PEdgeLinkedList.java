package com.arcadedb.graph;

import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.PRID;

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

  public void add(final PRID edgeRID, final PRID vertexRID) {
    if (first.add(edgeRID, vertexRID))
      ((PDatabaseInternal) vertex.getDatabase()).updateRecord(first);
    else {
//      if (direction == PVertex.DIRECTION.OUT)
//        PLogManager.instance()
//            .info(this, "Edge linked list chunk full, allocate a new one %s -(%s)-> vertex=%s)", vertex, edgeRID, vertexRID);
//      else
//        PLogManager.instance()
//            .info(this, "Edge linked list chunk full, allocate a new one %s <-(%s)- vertex=%s)", vertex, edgeRID, vertexRID);

      // CHUNK FULL, ALLOCATE A NEW ONE
      PDatabaseInternal database = (PDatabaseInternal) vertex.getDatabase();

      final PModifiableEdgeChunk newChunk = new PModifiableEdgeChunk(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);

      database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

      newChunk.setNext(first);
      database.updateRecord(newChunk);

      final PModifiableVertex modifiableV = (PModifiableVertex) vertex.modify();

      if (direction == PVertex.DIRECTION.OUT)
        modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
      else
        modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

      first = newChunk;

      modifiableV.save();
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
