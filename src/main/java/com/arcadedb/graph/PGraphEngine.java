package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.PBucket;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PMultiIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class PGraphEngine {
  public static final int EDGES_LINKEDLIST_CHUNK_SIZE = 30;

  public void createVertexType(PDatabaseInternal database, final PDocumentType type) {
    for (PBucket b : type.getBuckets()) {
      if (!database.getSchema().existsBucket(b.getName() + "_out_edges"))
        database.getSchema().createBucket(b.getName() + "_out_edges");
      if (!database.getSchema().existsBucket(b.getName() + "_in_edges"))
        database.getSchema().createBucket(b.getName() + "_in_edges");
    }
  }

  public PEdge newEdge(PVertexInternal vertex, final String edgeType, PIdentifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final PRID rid = vertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PDatabaseInternal database = (PDatabaseInternal) vertex.getDatabase();

    final PModifiableEdge edge = new PModifiableEdge(database, edgeType, rid, toVertex.getIdentity());
    setProperties(edge, properties);
    edge.save();

    PRID outEdgesHeadChunk = vertex.getOutEdgesHeadChunk();
    if (outEdgesHeadChunk == null) {
      final PModifiableEdgeChunk outChunk = new PModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
      database.createRecord(outChunk, getEdgesBucketName(database, rid.getBucketId(), PVertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      vertex = (PVertexInternal) vertex.modify();
      vertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
      database.updateRecord(vertex);
    }

    final PEdgeLinkedList outLinkedList = new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true));

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional) {
      PVertexInternal toVertexRecord = (PVertexInternal) toVertex.getRecord();

      PRID inEdgesHeadChunk = toVertexRecord.getInEdgesHeadChunk();
      if (inEdgesHeadChunk == null) {
        final PModifiableEdgeChunk inChunk = new PModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
        database.createRecord(inChunk, getEdgesBucketName(database, toVertex.getIdentity().getBucketId(), PVertex.DIRECTION.IN));
        inEdgesHeadChunk = inChunk.getIdentity();

        toVertexRecord = (PVertexInternal) toVertexRecord.modify();
        toVertexRecord.setInEdgesHeadChunk(inEdgesHeadChunk);
        database.updateRecord(toVertexRecord);
      }

      final PEdgeLinkedList inLinkedList = new PEdgeLinkedList(toVertexRecord, PVertex.DIRECTION.IN,
          (PEdgeChunk) database.lookupByRID(toVertexRecord.getInEdgesHeadChunk(), true));

      inLinkedList.add(edge.getIdentity(), rid);
    }

    return edge;
  }

  public long countEdges(final PVertexInternal vertex, final PVertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    long total = 0;

    switch (direction) {
    case BOTH:
      if (vertex.getOutEdgesHeadChunk() != null)
        total += new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).count(edgeType);
      if (vertex.getInEdgesHeadChunk() != null)
        total += new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).count(edgeType);
      break;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        total = new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).count(edgeType);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        total = new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).count(edgeType);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return total;
  }

  public Iterator<PEdge> getEdges(final PVertexInternal vertex) {
    final PMultiIterator<PEdge> result = new PMultiIterator<>();

    if (vertex.getOutEdgesHeadChunk() != null)
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());

    return result;
  }

  public Iterator<PEdge> getEdges(final PVertexInternal vertex, final PVertex.DIRECTION direction) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final PMultiIterator<PEdge> result = new PMultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator();
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator();
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return Collections.EMPTY_LIST.iterator();
  }

  public Iterator<PEdge> getEdges(final PVertexInternal vertex, final PVertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeType == null)
      throw new IllegalArgumentException("Edge Type is null");

    switch (direction) {
    case BOTH:
      final PMultiIterator<PEdge> result = new PMultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeType));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeType));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeType);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeType);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST.iterator();
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public Iterator<PVertex> getVertices(final PVertexInternal vertex) {
    final PMultiIterator<PVertex> result = new PMultiIterator<>();

    if (vertex.getOutEdgesHeadChunk() != null)
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());

    return result;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PVertex instances
   */
  public Iterator<PVertex> getVertices(final PVertexInternal vertex, final PVertex.DIRECTION direction) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final PMultiIterator<PVertex> result = new PMultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator();
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator();
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST.iterator();
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeType  Edge type name to filter
   *
   * @return An iterator of PVertex instances
   */
  public Iterator<PVertex> getVertices(final PVertexInternal vertex, final PVertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeType == null)
      throw new IllegalArgumentException("Edge Type is null");

    switch (direction) {
    case BOTH:
      final PMultiIterator<PVertex> result = new PMultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeType));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeType));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeType);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeType);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST.iterator();
  }

  public boolean isVertexConnectedTo(final PVertexInternal vertex, final PIdentifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (vertex.getOutEdgesHeadChunk() != null && new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
      return true;

    if (vertex.getInEdgesHeadChunk() != null)
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity());

    return false;
  }

  public boolean isVertexConnectedTo(final PVertexInternal vertex, final PIdentifiable toVertex,
      final PVertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == PVertex.DIRECTION.OUT | direction == PVertex.DIRECTION.BOTH)
      if (vertex.getOutEdgesHeadChunk() != null && new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
          .containsVertex(toVertex.getIdentity()))
        return true;

    if (direction == PVertex.DIRECTION.IN | direction == PVertex.DIRECTION.BOTH)
      if (vertex.getInEdgesHeadChunk() != null)
        return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
            (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .containsVertex(toVertex.getIdentity());

    return false;
  }

  public static String getEdgesBucketName(final PDatabase database, final int bucketId, final PVertex.DIRECTION direction) {
    final PBucket vertexBucket = database.getSchema().getBucketById(bucketId);

    if (direction == PVertex.DIRECTION.OUT)
      return vertexBucket.getName() + "_out_edges";
    else if (direction == PVertex.DIRECTION.IN)
      return vertexBucket.getName() + "_in_edges";

    throw new IllegalArgumentException("Invalid direction");
  }

  public static void setProperties(final PModifiableDocument edge, final Object[] properties) {
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
