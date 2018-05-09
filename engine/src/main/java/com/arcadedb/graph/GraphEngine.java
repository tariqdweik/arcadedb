/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.MultiIterator;

import java.util.Collections;
import java.util.Map;

public class GraphEngine {
  public static final int EDGES_LINKEDLIST_CHUNK_SIZE = 30;

  public void createVertexType(DatabaseInternal database, final DocumentType type) {
    for (Bucket b : type.getBuckets(false)) {
      if (!database.getSchema().existsBucket(b.getName() + "_out_edges"))
        database.getSchema().createBucket(b.getName() + "_out_edges");
      if (!database.getSchema().existsBucket(b.getName() + "_in_edges"))
        database.getSchema().createBucket(b.getName() + "_in_edges");
    }
  }

  public Edge newEdge(VertexInternal vertex, final String edgeType, Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID rid = vertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof ModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

    final ModifiableEdge edge = new ModifiableEdge(database, edgeType, rid, toVertex.getIdentity());
    setProperties(edge, properties);
    edge.save();

    RID outEdgesHeadChunk = vertex.getOutEdgesHeadChunk();
    if (outEdgesHeadChunk == null) {
      final ModifiableEdgeChunk outChunk = new ModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
      database.createRecord(outChunk, getEdgesBucketName(database, rid.getBucketId(), Vertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      vertex = (VertexInternal) vertex.modify();
      vertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
      database.updateRecord(vertex);
    }

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
        (EdgeChunk) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true));

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional) {
      VertexInternal toVertexRecord = (VertexInternal) toVertex.getRecord();

      RID inEdgesHeadChunk = toVertexRecord.getInEdgesHeadChunk();
      if (inEdgesHeadChunk == null) {
        final ModifiableEdgeChunk inChunk = new ModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
        database.createRecord(inChunk, getEdgesBucketName(database, toVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
        inEdgesHeadChunk = inChunk.getIdentity();

        toVertexRecord = (VertexInternal) toVertexRecord.modify();
        toVertexRecord.setInEdgesHeadChunk(inEdgesHeadChunk);
        database.updateRecord(toVertexRecord);
      }

      final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN,
          (EdgeChunk) database.lookupByRID(toVertexRecord.getInEdgesHeadChunk(), true));

      inLinkedList.add(edge.getIdentity(), rid);
    }

    return edge;
  }

  public long countEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    long total = 0;

    switch (direction) {
    case BOTH:
      if (vertex.getOutEdgesHeadChunk() != null)
        total += new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).count(edgeType);
      if (vertex.getInEdgesHeadChunk() != null)
        total += new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).count(edgeType);
      break;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        total = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).count(edgeType);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        total = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).count(edgeType);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return total;
  }

  public void deleteEdge(final Edge edge) {
    final Database database = edge.getDatabase();

    final VertexInternal vOut = (VertexInternal) edge.getOutVertex();

    if (vOut != null) {
      if (vOut.getOutEdgesHeadChunk() != null)
        new EdgeLinkedList(vOut, Vertex.DIRECTION.OUT, (EdgeChunk) database.lookupByRID(vOut.getOutEdgesHeadChunk(), true))
            .removeEdge(edge.getIdentity());
    }

    final VertexInternal vIn = (VertexInternal) edge.getInVertex();
    if (vIn != null) {
      if (vIn.getInEdgesHeadChunk() != null)
        new EdgeLinkedList(vIn, Vertex.DIRECTION.IN, (EdgeChunk) database.lookupByRID(vIn.getInEdgesHeadChunk(), true))
            .removeEdge(edge.getIdentity());
    }

    // DELETE EDGE RECORD
    database.getSchema().getBucketById(edge.getIdentity().getBucketId()).deleteRecord(edge.getIdentity());
  }

  public void deleteVertex(final VertexInternal vertex) {
    final Database database = vertex.getDatabase();

    if (vertex.getOutEdgesHeadChunk() != null) {
      final EdgeIterator outIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator();

      while (outIterator.hasNext()) {
        final Edge nextEdge = outIterator.next();
        VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();
        if (nextVertex.getInEdgesHeadChunk() != null) {
          new EdgeLinkedList(nextVertex, Vertex.DIRECTION.IN,
              (EdgeChunk) database.lookupByRID(nextVertex.getInEdgesHeadChunk(), true)).removeEdge(nextEdge.getIdentity());
          database.getSchema().getBucketById(nextEdge.getIdentity().getBucketId()).deleteRecord(nextEdge.getIdentity());
        }
      }
    }

    if (vertex.getInEdgesHeadChunk() != null) {
      final EdgeIterator inIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
          (EdgeChunk) database.lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator();

      while (inIterator.hasNext()) {
        final Edge nextEdge = inIterator.next();
        VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();
        if (nextVertex.getOutEdgesHeadChunk() != null) {
          new EdgeLinkedList(nextVertex, Vertex.DIRECTION.OUT,
              (EdgeChunk) database.lookupByRID(nextVertex.getOutEdgesHeadChunk(), true)).removeEdge(nextEdge.getIdentity());
          database.getSchema().getBucketById(nextEdge.getIdentity().getBucketId()).deleteRecord(nextEdge.getIdentity());
        }
      }
    }

    // DELETE VERTEX RECORD
    vertex.getDatabase().getSchema().getBucketById(vertex.getIdentity().getBucketId()).deleteRecord(vertex.getIdentity());
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex) {
    final MultiIterator<Edge> result = new MultiIterator<>();

    if (vertex.getOutEdgesHeadChunk() != null)
      result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());

    return result;
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final MultiIterator<Edge> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator();
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator();
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return Collections.EMPTY_LIST;
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String[] edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeTypes == null || edgeTypes.length == 0)
      throw new IllegalArgumentException("Edge Type is empty");

    switch (direction) {
    case BOTH:
      final MultiIterator<Edge> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeTypes));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeTypes));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeTypes);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST;
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex) {
    final MultiIterator<Vertex> result = new MultiIterator<>();

    if (vertex.getOutEdgesHeadChunk() != null)
      result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());

    return result;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final MultiIterator<Vertex> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator();
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator();
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return Collections.EMPTY_LIST;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeTypes Edge type names to filter
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex, final Vertex.DIRECTION direction, final String edgeTypes[]) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeTypes == null || edgeTypes.length == 0)
      throw new IllegalArgumentException("Edge Type is empty");

    switch (direction) {
    case BOTH:
      final MultiIterator<Vertex> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeTypes));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeTypes));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeTypes);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (vertex.getOutEdgesHeadChunk() != null && new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
        (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
      return true;

    if (vertex.getInEdgesHeadChunk() != null)
      return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity());

    return false;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex,
      final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == Vertex.DIRECTION.OUT | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getOutEdgesHeadChunk() != null && new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
          .containsVertex(toVertex.getIdentity()))
        return true;

    if (direction == Vertex.DIRECTION.IN | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .containsVertex(toVertex.getIdentity());

    return false;
  }

  public static String getEdgesBucketName(final Database database, final int bucketId, final Vertex.DIRECTION direction) {
    final Bucket vertexBucket = database.getSchema().getBucketById(bucketId);

    if (direction == Vertex.DIRECTION.OUT)
      return vertexBucket.getName() + "_out_edges";
    else if (direction == Vertex.DIRECTION.IN)
      return vertexBucket.getName() + "_in_edges";

    throw new IllegalArgumentException("Invalid direction");
  }

  public static void setProperties(final ModifiableDocument edge, final Object[] properties) {
    if (properties != null)
      if (properties.length == 1 && properties[0] instanceof Map) {
        // GET PROPERTIES FROM THE MAP
        final Map<String, Object> map = (Map<String, Object>) properties[0];
        for (Map.Entry<String, Object> entry : map.entrySet())
          edge.set(entry.getKey(), entry.getValue());
      } else {
        if (properties.length % 2 != 0)
          throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
        for (int i = 0; i < properties.length; i += 2)
          edge.set((String) properties[i], properties[i + 1]);
      }
  }
}
