/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.logging.Level;

public class GraphEngine {
  public static final int EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE = 64;

  public static class CreateEdgeOperation {
    final String       edgeTypeName;
    final Identifiable destinationVertex;
    final Object[]     edgeProperties;

    public CreateEdgeOperation(final String edgeTypeName, final Identifiable destinationVertex, final Object[] edgeProperties) {
      this.edgeTypeName = edgeTypeName;
      this.destinationVertex = destinationVertex;
      this.edgeProperties = edgeProperties;
    }
  }

  public void createVertexType(DatabaseInternal database, final DocumentType type) {
    for (Bucket b : type.getBuckets(false)) {
      if (!database.getSchema().existsBucket(b.getName() + "_out_edges"))
        database.getSchema().createBucket(b.getName() + "_out_edges");
      if (!database.getSchema().existsBucket(b.getName() + "_in_edges"))
        database.getSchema().createBucket(b.getName() + "_in_edges");
    }
  }

  public ImmutableLightEdge newLightEdge(VertexInternal fromVertex, final String edgeType, Identifiable toVertex, final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final RID edgeRID = new RID(database, database.getSchema().getType(edgeType).getFirstBucketId(), -1l);

    final ImmutableLightEdge edge = new ImmutableLightEdge(database, edgeType, edgeRID, fromVertexRID, toVertex.getIdentity());

    connectEdge(database, fromVertex, toVertex, edge, bidirectional);

    return edge;
  }

  public MutableEdge newEdge(final VertexInternal fromVertex, final String edgeType, Identifiable toVertex, final boolean bidirectional,
      final Object... edgeProperties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final MutableEdge edge = new MutableEdge(database, edgeType, fromVertexRID, toVertex.getIdentity());
    if (edgeProperties != null && edgeProperties.length > 0)
      setProperties(edge, edgeProperties);

    edge.save();

    connectEdge(database, fromVertex, toVertex, edge, bidirectional);

    return edge;
  }

  public void connectEdge(final DatabaseInternal database, VertexInternal fromVertex, final Identifiable toVertex, final Edge edge,
      final boolean bidirectional) {
    fromVertex = fromVertex.modify();

    final EdgeSegment outChunk = createOutEdgeChunk(database, (MutableVertex) fromVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional)
      connectIncomingEdge(database, toVertex, fromVertex.getIdentity(), edge.getIdentity());
  }

  public void upgradeEdge(final DatabaseInternal database, VertexInternal fromVertex, final Identifiable toVertex, final MutableEdge edge,
      final boolean bidirectional) {
    fromVertex = fromVertex.modify();
    final EdgeSegment outChunk = createOutEdgeChunk(database, (MutableVertex) fromVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.upgrade(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional)
      upgradeIncomingEdge(database, toVertex, fromVertex.getIdentity(), edge.getIdentity());
  }

  public void upgradeIncomingEdge(final DatabaseInternal database, final Identifiable toVertex, final RID fromVertexRID, final RID edgeRID) {
    final MutableVertex toVertexRecord = ((VertexInternal) toVertex.getRecord()).modify();

    final EdgeSegment inChunk = createInEdgeChunk(database, toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.upgrade(edgeRID, fromVertexRID);
  }

  public List<Edge> newEdges(final DatabaseInternal database, VertexInternal sourceVertex, final List<CreateEdgeOperation> connections,
      final boolean bidirectional) {

    if (connections == null || connections.isEmpty())
      return Collections.EMPTY_LIST;

    final RID sourceVertexRID = sourceVertex.getIdentity();

    final List<Edge> edges = new ArrayList<>(connections.size());
    final List<Pair<Identifiable, Identifiable>> outEdgePairs = new ArrayList<>();

    for (int i = 0; i < connections.size(); ++i) {
      final CreateEdgeOperation connection = connections.get(i);

      final MutableEdge edge;

      final Identifiable destinationVertex = connection.destinationVertex;

      edge = new MutableEdge(database, connection.edgeTypeName, sourceVertexRID, destinationVertex.getIdentity());

      if (connection.edgeProperties != null && connection.edgeProperties.length > 0)
        setProperties(edge, connection.edgeProperties);

      edge.save();

      outEdgePairs.add(new Pair<>(edge, destinationVertex));

      edges.add(edge);
    }

    sourceVertex = sourceVertex.modify();

    final EdgeSegment outChunk = createOutEdgeChunk(database, (MutableVertex) sourceVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(sourceVertex, Vertex.DIRECTION.OUT, outChunk);
    outLinkedList.addAll(outEdgePairs);

    if (bidirectional) {
      for (int i = 0; i < outEdgePairs.size(); ++i) {
        final Pair<Identifiable, Identifiable> edge = outEdgePairs.get(i);
        connectIncomingEdge(database, edge.getSecond(), edge.getFirst().getIdentity(), sourceVertexRID);
      }
    }

    return edges;
  }

  public void connectIncomingEdge(final DatabaseInternal database, final Identifiable toVertex, final RID fromVertexRID, final RID edgeRID) {
    final MutableVertex toVertexRecord = ((VertexInternal) toVertex.getRecord()).modify();

    final EdgeSegment inChunk = createInEdgeChunk(database, toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.add(edgeRID, fromVertexRID);
  }

  public EdgeSegment createInEdgeChunk(final DatabaseInternal database, final MutableVertex toVertex) {
    RID inEdgesHeadChunk = toVertex.getInEdgesHeadChunk();

    EdgeSegment inChunk = null;
    if (inEdgesHeadChunk != null)
      try {
        inChunk = (EdgeSegment) database.lookupByRID(inEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Record %s (inEdgesHeadChunk) not found on vertex %s. Creating a new one", null, inEdgesHeadChunk, toVertex);
        inEdgesHeadChunk = null;
      }

    if (inEdgesHeadChunk == null) {
      inChunk = new MutableEdgeSegment(database, EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE);
      database.createRecord(inChunk, getEdgesBucketName(database, toVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
      inEdgesHeadChunk = inChunk.getIdentity();

      toVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
      database.updateRecord(toVertex);
    }

    return inChunk;
  }

  public EdgeSegment createOutEdgeChunk(final DatabaseInternal database, final MutableVertex fromVertex) {
    RID outEdgesHeadChunk = fromVertex.getOutEdgesHeadChunk();

    EdgeSegment outChunk = null;
    if (outEdgesHeadChunk != null)
      try {
        outChunk = (EdgeSegment) database.lookupByRID(outEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance().log(this, Level.WARNING, "Record %s (outEdgesHeadChunk) not found on vertex %s. Creating a new one", null, outEdgesHeadChunk,
            fromVertex.getIdentity());
        outEdgesHeadChunk = null;
      }

    if (outEdgesHeadChunk == null) {
      outChunk = new MutableEdgeSegment(database, EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE);
      database.createRecord(outChunk, getEdgesBucketName(database, fromVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      fromVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
      database.updateRecord(fromVertex);
    }

    return outChunk;
  }

  public long countEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    long total = 0;

    switch (direction) {
    case BOTH:
      if (vertex.getOutEdgesHeadChunk() != null)
        total += new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
            .count(edgeType);
      if (vertex.getInEdgesHeadChunk() != null)
        total += new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .count(edgeType);
      break;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        total = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
            .count(edgeType);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        total = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .count(edgeType);
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
        new EdgeLinkedList(vOut, Vertex.DIRECTION.OUT, (EdgeSegment) database.lookupByRID(vOut.getOutEdgesHeadChunk(), true)).removeEdge(edge);
    }

    final VertexInternal vIn = (VertexInternal) edge.getInVertex();
    if (vIn != null) {
      if (vIn.getInEdgesHeadChunk() != null)
        new EdgeLinkedList(vIn, Vertex.DIRECTION.IN, (EdgeSegment) database.lookupByRID(vIn.getInEdgesHeadChunk(), true)).removeEdge(edge);
    }

    final RID edgeRID = edge.getIdentity();
    if (edgeRID != null && !(edge instanceof LightEdge))
      // DELETE EDGE RECORD TOO
      database.getSchema().getBucketById(edge.getIdentity().getBucketId()).deleteRecord(edge.getIdentity());
  }

  public void deleteVertex(final VertexInternal vertex) {
    final Database database = vertex.getDatabase();

    final RID outRID = vertex.getOutEdgesHeadChunk();
    if (outRID != null) {
      final Iterator<Edge> outIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) database.lookupByRID(outRID, true)).edgeIterator();

      while (outIterator.hasNext()) {
        final Edge nextEdge = outIterator.next();
        try {
          VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();
          if (nextVertex.getInEdgesHeadChunk() != null) {
            new EdgeLinkedList(nextVertex, Vertex.DIRECTION.IN, (EdgeSegment) database.lookupByRID(nextVertex.getInEdgesHeadChunk(), true))
                .removeEdge(nextEdge);

            if (nextEdge.getIdentity().getPosition() > -1)
              // NON LIGHTWEIGHT
              database.getSchema().getBucketById(nextEdge.getIdentity().getBucketId()).deleteRecord(nextEdge.getIdentity());
          }
        } catch (RecordNotFoundException e) {
          // ALREADY DELETED, IGNORE THIS
          LogManager.instance()
              .log(this, Level.FINE, "Error on deleting outgoing vertex %s connected from vertex %s (record not found)", null, nextEdge.getIn(),
                  vertex.getIdentity());
        }
      }

      database.getSchema().getBucketById(outRID.getIdentity().getBucketId()).deleteRecord(outRID.getIdentity());
    }

    final RID inRID = vertex.getInEdgesHeadChunk();
    if (inRID != null) {
      final Iterator<Edge> inIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) database.lookupByRID(inRID, true)).edgeIterator();

      while (inIterator.hasNext()) {
        final Edge nextEdge = inIterator.next();
        try {
          VertexInternal nextVertex = (VertexInternal) nextEdge.getOutVertex();
          if (nextVertex.getOutEdgesHeadChunk() != null) {
            new EdgeLinkedList(nextVertex, Vertex.DIRECTION.OUT, (EdgeSegment) database.lookupByRID(nextVertex.getOutEdgesHeadChunk(), true))
                .removeEdge(nextEdge);

            if (nextEdge.getIdentity().getPosition() > -1)
              // NON LIGHTWEIGHT
              database.getSchema().getBucketById(nextEdge.getIdentity().getBucketId()).deleteRecord(nextEdge.getIdentity());
          }
        } catch (RecordNotFoundException e) {
          // ALREADY DELETED, IGNORE THIS
          LogManager.instance()
              .log(this, Level.WARNING, "Error on deleting incoming vertex %s connected to vertex %s", null, nextEdge.getOut(), vertex.getIdentity());
        }
      }

      database.getSchema().getBucketById(inRID.getIdentity().getBucketId()).deleteRecord(inRID.getIdentity());
    }

    // DELETE VERTEX RECORD
    vertex.getDatabase().getSchema().getBucketById(vertex.getIdentity().getBucketId()).deleteRecord(vertex.getIdentity());
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex) {
    final MultiIterator<Edge> result = new MultiIterator<>();

    if (vertex.getOutEdgesHeadChunk() != null)
      result.add(
          new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(
          new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator());

    return result;
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final MultiIterator<Edge> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
            .edgeIterator(edgeTypes));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .edgeIterator(edgeTypes));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return (Iterable<Edge>) new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return (Iterable<Edge>) new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator(edgeTypes);
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
      result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
          .vertexIterator());

    if (vertex.getInEdgesHeadChunk() != null)
      result.add(
          new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator());

    return result;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeTypes Edge type names to filter
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH:
      final MultiIterator<Vertex> result = new MultiIterator<>();
      if (vertex.getOutEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true))
            .vertexIterator(edgeTypes));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
            .vertexIterator(edgeTypes));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return (Iterable<Vertex>) new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return (Iterable<Vertex>) new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeTypes);
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
        (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
      return true;

    if (vertex.getInEdgesHeadChunk() != null)
      return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
          .containsVertex(toVertex.getIdentity());

    return false;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex, final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == Vertex.DIRECTION.OUT | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getOutEdgesHeadChunk() != null && new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
        return true;

    if (direction == Vertex.DIRECTION.IN | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
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

  public static void setProperties(final MutableDocument edge, final Object[] properties) {
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
