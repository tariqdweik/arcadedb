/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.CompressedRID2RIDsIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class GraphEngine {
  public static final int EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE = 64;
  //private static final RID NO_RID                              = new RID(null, -1, -1);

  public void createVertexType(DatabaseInternal database, final DocumentType type) {
    for (Bucket b : type.getBuckets(false)) {
      if (!database.getSchema().existsBucket(b.getName() + "_out_edges"))
        database.getSchema().createBucket(b.getName() + "_out_edges");
      if (!database.getSchema().existsBucket(b.getName() + "_in_edges"))
        database.getSchema().createBucket(b.getName() + "_in_edges");
    }
  }

  public MutableEdge newEdge(VertexInternal fromVertex, final String edgeType, Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final MutableEdge edge;

//    if (properties != null && properties.length > 0) {
    edge = new MutableEdge(database, edgeType, fromVertexRID, toVertex.getIdentity());
    setProperties(edge, properties);
    edge.save();
//    } else {
//      // TODO: MANAGE NO RID WITH THE CREATION OF A NEW ONE AT THE FIRST PROPERTY
//      edge = new MutableEdge(database, edgeType, fromVertexRID, toVertex.getIdentity());
//      edge.setIdentity(NO_RID);
//    }

    final AtomicReference<VertexInternal> fromVertexRef = new AtomicReference<>(fromVertex);
    final EdgeChunk outChunk = createOutEdgeChunk(database, fromVertexRef);
    fromVertex = fromVertexRef.get();

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional)
      connectIncomingEdge(database, toVertex, fromVertexRID, edge.getIdentity());

    return edge;
  }

  public void newEdges(final DatabaseInternal database, VertexInternal sourceVertex, final List<Pair<Identifiable, Object[]>> connections,
      final String edgeType, final boolean bidirectional) {
    if (connections == null || connections.isEmpty())
      return;

    final RID sourceVertexRID = sourceVertex.getIdentity();

    final List<Pair<Identifiable, Identifiable>> outEdgePairs = new ArrayList<>();

    for (int i = 0; i < connections.size(); ++i) {
      final Pair<Identifiable, Object[]> connection = connections.get(i);

      final MutableEdge edge;

      final Identifiable destinationVertex = connection.getFirst();

//      if (connection.getSecond() != null && connection.getSecond().length > 0) {
      edge = new MutableEdge(database, edgeType, sourceVertexRID, destinationVertex.getIdentity());
      setProperties(edge, connection.getSecond());
      edge.save();
//      } else {
//        // TODO: MANAGE NO RID WITH THE CREATION OF A NEW ONE AT THE FIRST PROPERTY
//        edge = new MutableEdge(database, edgeType, sourceVertexRID, destinationVertex.getIdentity());
//        edge.setIdentity(NO_RID);
//      }

      outEdgePairs.add(new Pair<>(edge, destinationVertex));
    }

    final AtomicReference<VertexInternal> v = new AtomicReference<>(sourceVertex);
    final EdgeChunk outChunk = createOutEdgeChunk(database, v);
    sourceVertex = v.get();

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(sourceVertex, Vertex.DIRECTION.OUT, outChunk);
    outLinkedList.addAll(outEdgePairs);

    if (bidirectional) {
      for (int i = 0; i < outEdgePairs.size(); ++i) {
        final Pair<Identifiable, Identifiable> edge = outEdgePairs.get(i);
        connectIncomingEdge(database, edge.getSecond(), edge.getFirst().getIdentity(), sourceVertexRID);
      }
    }
  }

  public void createIncomingConnectionsInBatch(final DatabaseInternal database, final String vertexTypeName, final String edgeTypeName) {
    final DocumentType type = database.getSchema().getType(vertexTypeName);
    if (!(type instanceof VertexType))
      throw new IllegalArgumentException("Type '" + vertexTypeName + "' is not a vertex");

    final CompressedRID2RIDsIndex index = new CompressedRID2RIDsIndex(database, 10 * 1024 * 1024);

    final AtomicLong browsedVertices = new AtomicLong();
    final AtomicLong browsedEdges = new AtomicLong();

    database.scanType(vertexTypeName, false, new DocumentCallback() {
      @Override
      public boolean onRecord(final Document record) {
        final VertexInternal v = (VertexInternal) record;

        browsedVertices.incrementAndGet();

        final RID out = v.getOutEdgesHeadChunk();
        if (out != null) {
          final EdgeLinkedList edgeList = new EdgeLinkedList(v, Vertex.DIRECTION.OUT, (EdgeChunk) database.lookupByRID(out, true));
          final Iterator<Pair<RID, RID>> iterator = edgeList.entryIterator(edgeTypeName);
          while (iterator.hasNext()) {
            final Pair<RID, RID> entry = iterator.next();

            index.put(entry.getSecond(), entry.getFirst(), v.getIdentity());

            browsedEdges.incrementAndGet();

            if (index.getChunkSize() > 256 * 1024 * 1024) {
              LogManager.instance().log(this, Level.INFO,
                  "Creation of back connections, reached %s size, flushing %d connections (from %d vertices and %d edges)...", null,
                  FileUtils.getSizeAsString(index.getChunkSize()), index.size(), browsedVertices.get(), browsedEdges.get());

              createIncomingEdgesInBatch(database, index, edgeTypeName);

              LogManager.instance().log(this, Level.INFO, "Creation done, reset index buffer and continue", null);

              // CREATE A NEW CHUNK BEFORE CONTINUING
              index.reset();
            }
          }
        }

        return true;
      }
    });

    createIncomingEdgesInBatch(database, index, edgeTypeName);
  }

  private void createIncomingEdgesInBatch(final DatabaseInternal database, final CompressedRID2RIDsIndex index, final String edgeTypeName) {
    Vertex lastVertex = null;
    final List<Pair<Identifiable, Identifiable>> connections = new ArrayList<>();

    long totalVertices = 0;
    long totalEdges = 0;
    int minEdges = Integer.MAX_VALUE;
    int maxEdges = -1;

    for (final CompressedRID2RIDsIndex.EntryIterator it = index.entryIterator(); it.hasNext(); it.moveNext()) {
      final Vertex destinationVertex = it.getKey().getVertex(true);

      if (!connections.isEmpty() && !destinationVertex.equals(lastVertex)) {
        ++totalVertices;

        if (connections.size() < minEdges)
          minEdges = connections.size();
        if (connections.size() > maxEdges)
          maxEdges = connections.size();

        connectIncomingEdges(database, lastVertex, connections, edgeTypeName);
        connections.clear();
      }

      lastVertex = destinationVertex;

      connections.add(new Pair<>(it.getEdge(), it.getVertex()));

      ++totalEdges;

      if (totalEdges % 10000 == 0) {
        // BATCH
        database.commit();
        database.begin();
      }

    }

    if (lastVertex != null)
      connectIncomingEdges(database, lastVertex, connections, edgeTypeName);

    LogManager.instance()
        .log(this, Level.INFO, "Created %d back connections from %d vertices (min=%d max=%d avg=%d)", null, totalEdges, totalVertices,
            minEdges, maxEdges, totalVertices > 0 ? totalEdges / totalVertices : 0);
  }

  private void connectIncomingEdges(final DatabaseInternal database, final Identifiable toVertex,
      final List<Pair<Identifiable, Identifiable>> connections, final String edgeType) {
    VertexInternal toVertexRecord = (VertexInternal) toVertex.getRecord();

    final AtomicReference<VertexInternal> toVertexRef = new AtomicReference<>(toVertexRecord);
    final EdgeChunk inChunk = createInEdgeChunk(database, toVertexRef);
    toVertexRecord = toVertexRef.get();

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.addAll(connections);
  }

  public void connectIncomingEdge(final DatabaseInternal database, final Identifiable toVertex, final RID fromVertexRID,
      final RID edgeRID) {
    VertexInternal toVertexRecord = (VertexInternal) toVertex.getRecord();

    final AtomicReference<VertexInternal> toVertexRef = new AtomicReference<>(toVertexRecord);
    final EdgeChunk inChunk = createInEdgeChunk(database, toVertexRef);
    toVertexRecord = toVertexRef.get();

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.add(edgeRID, fromVertexRID);
  }

  public EdgeChunk createInEdgeChunk(final DatabaseInternal database, final AtomicReference<VertexInternal> vertex) {
    VertexInternal toVertex = vertex.get();

    RID inEdgesHeadChunk = toVertex.getInEdgesHeadChunk();

    EdgeChunk inChunk = null;
    if (inEdgesHeadChunk != null)
      try {
        inChunk = (EdgeChunk) database.lookupByRID(inEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Record %s (inEdgesHeadChunk) not found on vertex %s. Creating a new one", null, inEdgesHeadChunk,
                toVertex);
        inEdgesHeadChunk = null;
      }

    if (inEdgesHeadChunk == null) {
      inChunk = new MutableEdgeChunk(database, EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE);
      database.createRecord(inChunk, getEdgesBucketName(database, toVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
      inEdgesHeadChunk = inChunk.getIdentity();

      toVertex = toVertex.modify();
      vertex.set(toVertex);
      toVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
      database.updateRecord(toVertex);
    }

    return inChunk;
  }

  public EdgeChunk createOutEdgeChunk(final DatabaseInternal database, final AtomicReference<VertexInternal> vertex) {
    VertexInternal fromVertex = vertex.get();

    RID outEdgesHeadChunk = fromVertex.getOutEdgesHeadChunk();

    EdgeChunk outChunk = null;
    if (outEdgesHeadChunk != null)
      try {
        outChunk = (EdgeChunk) database.lookupByRID(outEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Record %s (outEdgesHeadChunk) not found on vertex %s. Creating a new one", null, outEdgesHeadChunk,
                fromVertex.getIdentity());
        outEdgesHeadChunk = null;
      }

    if (outEdgesHeadChunk == null) {
      outChunk = new MutableEdgeChunk(database, EDGES_LINKEDLIST_INITIAL_CHUNK_SIZE);
      database.createRecord(outChunk, getEdgesBucketName(database, fromVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      fromVertex = fromVertex.modify();
      vertex.set(fromVertex);
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
      final Iterator<Edge> outIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator();

      while (outIterator.hasNext()) {
        final Edge nextEdge = outIterator.next();
        VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();
        if (nextVertex.getInEdgesHeadChunk() != null) {
          new EdgeLinkedList(nextVertex, Vertex.DIRECTION.IN, (EdgeChunk) database.lookupByRID(nextVertex.getInEdgesHeadChunk(), true))
              .removeEdge(nextEdge.getIdentity());
          database.getSchema().getBucketById(nextEdge.getIdentity().getBucketId()).deleteRecord(nextEdge.getIdentity());
        }
      }
    }

    if (vertex.getInEdgesHeadChunk() != null) {
      final Iterator<Edge> inIterator = new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
          (EdgeChunk) database.lookupByRID(vertex.getInEdgesHeadChunk(), true)).edgeIterator();

      while (inIterator.hasNext()) {
        final Edge nextEdge = inIterator.next();
        VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();
        if (nextVertex.getOutEdgesHeadChunk() != null) {
          new EdgeLinkedList(nextVertex, Vertex.DIRECTION.OUT, (EdgeChunk) database.lookupByRID(nextVertex.getOutEdgesHeadChunk(), true))
              .removeEdge(nextEdge.getIdentity());
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
      result.add(
          new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
              .edgeIterator());

    return result;
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

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
        return (Iterable<Edge>) new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).edgeIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return (Iterable<Edge>) new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
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
      result.add(
          new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true))
              .vertexIterator());

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
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeTypes));

      if (vertex.getInEdgesHeadChunk() != null)
        result.add(new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).vertexIterator(edgeTypes));
      return result;

    case OUT:
      if (vertex.getOutEdgesHeadChunk() != null)
        return (Iterable<Vertex>) new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).vertexIterator(edgeTypes);
      break;

    case IN:
      if (vertex.getInEdgesHeadChunk() != null)
        return (Iterable<Vertex>) new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
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

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex, final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == Vertex.DIRECTION.OUT | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getOutEdgesHeadChunk() != null && new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getOutEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity()))
        return true;

    if (direction == Vertex.DIRECTION.IN | direction == Vertex.DIRECTION.BOTH)
      if (vertex.getInEdgesHeadChunk() != null)
        return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN,
            (EdgeChunk) vertex.getDatabase().lookupByRID(vertex.getInEdgesHeadChunk(), true)).containsVertex(toVertex.getIdentity());

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
