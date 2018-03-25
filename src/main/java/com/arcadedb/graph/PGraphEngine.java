package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.PBucket;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PMultiIterator;

import java.util.Iterator;
import java.util.Map;

public class PGraphEngine {

  public void createVertexType(PDatabaseInternal database, final PDocumentType type) {
    for (PBucket b : type.getBuckets()) {
      if (!database.getSchema().existsBucket(b.getName() + "_out_edges"))
        database.getSchema().createBucket(b.getName() + "_out_edges");
      if (!database.getSchema().existsBucket(b.getName() + "_in_edges"))
        database.getSchema().createBucket(b.getName() + "_in_edges");
    }
  }

  public PEdge newEdge(final PVertexInternal vertex, final String edgeType, final PIdentifiable toVertex,
      final boolean bidirectional, final Object... properties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final PRID rid = vertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PDatabase database = vertex.getDatabase();

    final PModifiableEdge edge = new PModifiableEdge(database, edgeType, vertex.getIdentity(), toVertex.getIdentity());
    setProperties(edge, properties);
    edge.save();

    final PEdgeLinkedList outLinkedList = new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true));

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional) {
      final PVertexInternal toVertexRecord = (PVertexInternal) toVertex.getRecord();

      final PEdgeLinkedList inLinkedList = new PEdgeLinkedList(toVertexRecord, PVertex.DIRECTION.IN,
          (PEdgeChunk) database.lookupByRID(toVertexRecord.getInEdgesHeadChunk(), true));

      inLinkedList.add(edge.getIdentity(), rid);
    }

    return edge;
  }

  public Iterator<PEdge> getEdges(final PVertexInternal vertex) {
    final PMultiIterator<PEdge> result = new PMultiIterator<>();

    result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());
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
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());
      return result;

    case OUT:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator();

    case IN:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator();

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
  }

  public Iterator<PEdge> getEdges(final PVertexInternal vertex, final PVertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeType == null)
      throw new IllegalArgumentException("Edge Type is null");

    switch (direction) {
    case BOTH:
      final PMultiIterator<PEdge> result = new PMultiIterator<>();
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeType));
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeType));
      return result;

    case OUT:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeType);

    case IN:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeType);

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public Iterator<PVertex> getVertices(final PVertexInternal vertex) {
    final PMultiIterator<PVertex> result = new PMultiIterator<>();

    result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());
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
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator());
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());
      return result;

    case OUT:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator();

    case IN:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator();

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
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
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeType));
      result.add(new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeType));
      return result;

    case OUT:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeType);

    case IN:
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeType);

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
  }

  public boolean isVertexConnectedTo(final PVertexInternal vertex, final PIdentifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
        (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
      return true;

    return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
        (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity());
  }

  public boolean isVertexConnectedTo(final PVertexInternal vertex, final PIdentifiable toVertex,
      final PVertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == PVertex.DIRECTION.OUT | direction == PVertex.DIRECTION.BOTH)
      if (new PEdgeLinkedList(vertex, PVertex.DIRECTION.OUT,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
          .containsVertex(toVertex.getIdentity()))
        return true;

    if (direction == PVertex.DIRECTION.IN | direction == PVertex.DIRECTION.BOTH)
      return new PEdgeLinkedList(vertex, PVertex.DIRECTION.IN,
          (PEdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity());

    return false;
  }

  public String getEdgesBucketName(final PDatabase database, final int bucketId, final PVertex.DIRECTION direction) {
    final PBucket vertexBucket = database.getSchema().getBucketById(bucketId);

    if (direction == PVertex.DIRECTION.OUT)
      return vertexBucket.getName() + "_out_edges";
    else if (direction == PVertex.DIRECTION.IN)
      return vertexBucket.getName() + "_in_edges";

    throw new IllegalArgumentException("Invalid direction");
  }

  private void setProperties(final PModifiableDocument edge, final Object[] properties) {
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
