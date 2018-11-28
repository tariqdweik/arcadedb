/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.Pair;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EdgeLinkedList {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;
  private       EdgeChunk        first;

  public EdgeLinkedList(final Vertex vertex, final Vertex.DIRECTION direction, final EdgeChunk first) {
    this.vertex = vertex;
    this.direction = direction;
    this.first = first;
  }

  public Iterator<Pair<RID, RID>> entryIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeVertexIterator(first);
    return new EdgeVertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), first, edgeTypes);
  }

  public Iterator<Edge> edgeIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeIterator(first);
    return new EdgeIteratorFilter((DatabaseInternal) vertex.getDatabase(), first, edgeTypes);
  }

  public Iterator<Vertex> vertexIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new VertexIterator(first);
    return new VertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), first, edgeTypes);
  }

  public boolean containsEdge(final RID rid) {
    EdgeChunk current = first;
    while (current != null) {
      if (current.containsEdge(rid))
        return true;

      current = current.getNext();
    }

    return false;
  }

  public boolean containsVertex(final RID rid) {
    EdgeChunk current = first;
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
      final DocumentType type = vertex.getDatabase().getSchema().getType(edgeType);
      final List<Bucket> buckets = type.getBuckets(true);
      fileIdToFilter = new HashSet<Integer>(buckets.size());
      for (Bucket b : buckets)
        fileIdToFilter.add(b.getId());
    } else
      fileIdToFilter = null;

    EdgeChunk current = first;
    while (current != null) {
      total += current.count(fileIdToFilter);
      current = current.getNext();
    }

    return total;
  }

  public void add(final RID edgeRID, final RID vertexRID) {
    if (first.add(edgeRID, vertexRID))
      ((DatabaseInternal) vertex.getDatabase()).updateRecord(first);
    else {
      // CHUNK FULL, ALLOCATE A NEW ONE
      DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

      final MutableEdgeChunk newChunk = new MutableEdgeChunk(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setNext(first);

      database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

      final MutableVertex modifiableV = vertex.modify();

      if (direction == Vertex.DIRECTION.OUT)
        modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
      else
        modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

      first = newChunk;

      modifiableV.save();
    }
  }

  public void addAll(final List<Pair<Identifiable, Identifiable>> entries) {
    final Set<Record> recordsToUpdate = new HashSet<>();

    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

    Vertex currentVertex = vertex;

    for (int i = 0; i < entries.size(); ++i) {
      final Pair<Identifiable, Identifiable> entry = entries.get(i);

      final RID edgeRID = entry.getFirst().getIdentity();
      final RID vertexRID = entry.getSecond().getIdentity();

      if (first.add(edgeRID, vertexRID))
        recordsToUpdate.add(first);
      else {
        // CHUNK FULL, ALLOCATE A NEW ONE
        final MutableEdgeChunk newChunk = new MutableEdgeChunk(database, computeBestSize());

        newChunk.add(edgeRID, vertexRID);
        newChunk.setNext(first);

        database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

        final MutableVertex modifiableV = currentVertex.modify();
        currentVertex = modifiableV;

        if (direction == Vertex.DIRECTION.OUT)
          modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
        else
          modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

        first = newChunk;

        recordsToUpdate.add(modifiableV);
      }
    }

    for (Record r : recordsToUpdate)
      database.updateRecord(r);
  }

  public void removeEdge(final RID edgeRID) {
    EdgeChunk current = first;
    while (current != null) {
      if (current.removeEdge(edgeRID) > 0)
        ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);

      current = current.getNext();
    }
  }

  public void removeVertex(final RID vertexRID) {
    EdgeChunk current = first;
    while (current != null) {
      if (current.removeVertex(vertexRID) > 0)
        ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);

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
