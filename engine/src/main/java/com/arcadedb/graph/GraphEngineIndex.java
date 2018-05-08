package com.arcadedb.graph;

public class GraphEngineIndex {}
//public class PGraphEngineIndex implements PGraphEngine {
//  public static final String EDGES_INDEX_NAME = "edges";
//  public static final PRID   NULL_RID         = new PRID(null, -1, -1);
//
//  @Override
//  public void createVertexType(final PDatabaseInternal database, final PDocumentType type) {
//    if (!database.getSchema().existsIndex(EDGES_INDEX_NAME)) {
//      database.getSchema().createManualIndex(EDGES_INDEX_NAME,
//          new byte[] { PBinaryTypes.TYPE_COMPRESSED_RID, PBinaryTypes.TYPE_BYTE, PBinaryTypes.TYPE_INT,
//              PBinaryTypes.TYPE_COMPRESSED_RID }, 65536 * 10);
//    }
//  }
//
//  @Override
//  public void createEdgeType(final PDatabaseInternal database, final PDocumentType c) {
//  }
//
//  public PEdge newEdge(final PVertex vertex, final String edgeType, final PIdentifiable toVertex, final boolean bidirectional,
//      final Object... properties) {
//    if (toVertex == null)
//      throw new IllegalArgumentException("Destination vertex is null");
//
//    final PRID rid = vertex.getIdentity();
//    if (rid == null)
//      throw new IllegalArgumentException("Current vertex is not persistent");
//
//    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
//      throw new IllegalArgumentException("Target vertex is not persistent");
//
//    final PEdgeType type = getEdgeType(vertex, edgeType);
//
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Object[] outKeys = new Object[] { rid, (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(), toVertex };
//
//    final PModifiableEdge edge = new PModifiableEdge(database, edgeType, vertex.getIdentity(), toVertex.getIdentity());
//
//    PRID edgeRID;
//
//    if (properties == null || properties.length == 0)
//      // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
//      edgeRID = NULL_RID;
//    else {
//      // SAVE THE PROPERTIES, THE EDGE AS A PERSISTENT RECORD AND AS VALUE FOR THE INDEX
//      setProperties(edge, properties);
//      ((PDatabaseInternal) database).saveRecord(edge);
//      edgeRID = edge.getIdentity();
//    }
//
//    edgeIndex.put(outKeys, edgeRID);
//
//    if (bidirectional) {
//      final Object[] inKeys = new Object[] { toVertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId(),
//          rid };
//
//      edgeIndex.put(inKeys, edgeRID);
//    }
//
//    // NO IDENTITY YET, AN ACTUAL RECORD WILL BE CREATED WHEN THE FIRST PROPERTY IS CREATED
//    return edge;
//  }
//
//  public PCursor<PEdge> getEdges(final PVertex vertex) {
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final List<PEdge> result = new ArrayList<>();
//    try {
//      final Object[] keys = new Object[] { vertex.getIdentity() };
//      final PIndexCursor connections = edgeIndex.iterator(keys);
//
//      while (connections.hasNext()) {
//        connections.next();
//        final Object[] entryKeys = connections.getKeys();
//        final PRID edgeRID = (PRID) connections.getValue();
//        result.add(new PImmutableEdge(database, database.getSchema().getDictionary().getNameById((Integer) entryKeys[2]),
//            NULL_RID.equals(edgeRID) ? null : edgeRID, (PRID) entryKeys[0], (PRID) entryKeys[3]));
//      }
//
//    } catch (IOException e) {
//      throw new RuntimeException("Error on browsing edges for vertex " + vertex.getIdentity(), e);
//    }
//
//    return new PCursorCollection<PEdge>(result);
//  }
//
//  public PCursor<PEdge> getEdges(final PVertex vertex, final PVertex.DIRECTION direction) {
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Set<PEdge> result = new HashSet<>();
//    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal() };
//        final PIndexCursor connections = edgeIndex.iterator(keys);
//
//        while (connections.hasNext()) {
//          connections.next();
//          final Object[] entryKeys = connections.getKeys();
//          final PRID edgeRID = (PRID) connections.getValue();
//          result.add(new PImmutableEdge(database, database.getSchema().getDictionary().getNameById((Integer) entryKeys[2]),
//              NULL_RID.equals(edgeRID) ? null : edgeRID, (PRID) entryKeys[0], (PRID) entryKeys[3]));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing outgoing edges for vertex " + vertex.getIdentity(), e);
//      }
//    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal() };
//        final PIndexCursor connections = edgeIndex.iterator(keys);
//
//        while (connections.hasNext()) {
//          connections.next();
//          final Object[] entryKeys = connections.getKeys();
//          final PRID edgeRID = (PRID) connections.getValue();
//          result.add(new PImmutableEdge(database, database.getSchema().getDictionary().getNameById((Integer) entryKeys[2]),
//              NULL_RID.equals(edgeRID) ? null : edgeRID, (PRID) entryKeys[3], (PRID) entryKeys[0]));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing incoming edges for vertex " + vertex.getIdentity(), e);
//      }
//    }
//
//    return new PCursorCollection<PEdge>(result);
//  }
//
//  public PCursor<PEdge> getEdges(final PVertex vertex, final PVertex.DIRECTION direction, final String edgeType) {
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PEdgeType type = getEdgeType(vertex, edgeType);
//
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Set<PEdge> result = new HashSet<>();
//    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId() };
//        final PIndexCursor connections = edgeIndex.iterator(keys);
//
//        while (connections.hasNext()) {
//          connections.next();
//          final Object[] entryKeys = connections.getKeys();
//          final PRID edgeRID = (PRID) connections.getValue();
//          result.add(new PImmutableEdge(database, database.getSchema().getDictionary().getNameById((Integer) entryKeys[2]),
//              NULL_RID.equals(edgeRID) ? null : edgeRID, (PRID) entryKeys[0], (PRID) entryKeys[3]));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing outgoing edges for vertex " + vertex.getIdentity(), e);
//      }
//    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId() };
//        final PIndexCursor connections = edgeIndex.iterator(keys);
//
//        while (connections.hasNext()) {
//          connections.next();
//          final Object[] entryKeys = connections.getKeys();
//          final PRID edgeRID = (PRID) connections.getValue();
//          result.add(new PImmutableEdge(database, database.getSchema().getDictionary().getNameById((Integer) entryKeys[2]),
//              NULL_RID.equals(edgeRID) ? null : edgeRID, (PRID) entryKeys[3], (PRID) entryKeys[0]));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing incoming edges for vertex " + vertex.getIdentity(), e);
//      }
//    }
//
//    return new PCursorCollection<PEdge>(result);
//  }
//
//  /**
//   * Returns all the connected vertices, both directions, any edge type.
//   *
//   * @return An iterator of PIndexCursorEntry entries
//   */
//  public PCursor<PVertex> getVertices(final PVertex vertex) {
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Set<PVertex> result = new HashSet<>();
//    try {
//      final Object[] keys = new Object[] { vertex.getIdentity() };
//      final PIndexCursor connections = edgeIndex.iterator(keys);
//
//      while (connections.hasNext()) {
//        connections.next();
//        final Object[] entryKeys = connections.getKeys();
//        final PRID vertexRID = (PRID) entryKeys[3];
//        result.add(
//            new PImmutableVertex(database, database.getSchema().getTypeNameByBucketId(vertexRID.getBucketId()), vertexRID, null));
//      }
//
//    } catch (IOException e) {
//      throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
//    }
//
//    return new PCursorCollection<PVertex>(result);
//  }
//
//  /**
//   * Returns the connected vertices.
//   *
//   * @param direction Direction between OUT, IN or BOTH
//   * @param edgeType  Edge type name to filter
//   *
//   * @return An iterator of PIndexCursorEntry entries
//   */
//  public PCursor<PVertex> getVertices(final PVertex vertex, final PVertex.DIRECTION direction, final String edgeType) {
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PEdgeType type = getEdgeType(vertex, edgeType);
//
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Set<PVertex> result = new HashSet<>();
//    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId() };
//        final PIndexCursor outVertices = edgeIndex.iterator(keys);
//
//        while (outVertices.hasNext()) {
//          outVertices.next();
//          final Object[] entryKeys = outVertices.getKeys();
//          final PRID vertexRID = (PRID) entryKeys[3];
//          result.add(
//              new PImmutableVertex(database, database.getSchema().getTypeNameByBucketId(vertexRID.getBucketId()), vertexRID, null));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
//      }
//    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId() };
//        final PIndexCursor inVertices = edgeIndex.iterator(keys);
//
//        while (inVertices.hasNext()) {
//          inVertices.next();
//          final Object[] entryKeys = inVertices.getKeys();
//          final PRID vertexRID = (PRID) entryKeys[3];
//          result.add(
//              new PImmutableVertex(database, database.getSchema().getTypeNameByBucketId(vertexRID.getBucketId()), vertexRID, null));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing incoming vertices for vertex " + vertex.getIdentity(), e);
//      }
//    }
//
//    return new PCursorCollection<PVertex>(result);
//  }
//
//  /**
//   * Returns the connected vertices.
//   *
//   * @param direction Direction between OUT, IN or BOTH
//   *
//   * @return An iterator of PIndexCursorEntry entries
//   */
//  public PCursor<PVertex> getVertices(final PVertex vertex, final PVertex.DIRECTION direction) {
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PDatabase database = vertex.getDatabase();
//
//    final PIndex edgeIndex = database.getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    final Set<PVertex> result = new HashSet<>();
//    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal() };
//        final PIndexCursor outVertices = edgeIndex.iterator(keys);
//
//        while (outVertices.hasNext()) {
//          outVertices.next();
//          final Object[] entryKeys = outVertices.getKeys();
//          final PRID vertexRID = (PRID) entryKeys[3];
//          result.add(
//              new PImmutableVertex(database, database.getSchema().getTypeNameByBucketId(vertexRID.getBucketId()), vertexRID, null));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
//      }
//    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
//      try {
//        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal() };
//        final PIndexCursor inVertices = edgeIndex.iterator(keys);
//
//        while (inVertices.hasNext()) {
//          inVertices.next();
//          final Object[] entryKeys = inVertices.getKeys();
//          final PRID vertexRID = (PRID) entryKeys[3];
//          result.add(
//              new PImmutableVertex(database, database.getSchema().getTypeNameByBucketId(vertexRID.getBucketId()), vertexRID, null));
//        }
//
//      } catch (IOException e) {
//        throw new RuntimeException("Error on browsing incoming vertices for vertex " + vertex.getIdentity(), e);
//      }
//    }
//
//    return new PCursorCollection<PVertex>(result);
//  }
//
//  public boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex) {
//    if (toVertex == null)
//      throw new IllegalArgumentException("toVertex is null");
//
//    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    try {
//      final Object[] keys = new Object[] { vertex.getIdentity() };
//      final PIndexCursor outVertices = edgeIndex.iterator(keys);
//
//      while (outVertices.hasNext()) {
//        outVertices.next();
//        final Object[] entryKeys = outVertices.getKeys();
//        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
//          return true;
//      }
//
//    } catch (IOException e) {
//      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
//    }
//    return false;
//  }
//
//  public boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex, final PVertex.DIRECTION direction) {
//    if (toVertex == null)
//      throw new IllegalArgumentException("toVertex is null");
//
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    try {
//      final Object[] keys = new Object[] { vertex.getIdentity(), (byte) direction.ordinal() };
//      final PIndexCursor outVertices = edgeIndex.iterator(keys);
//
//      while (outVertices.hasNext()) {
//        outVertices.next();
//        final Object[] entryKeys = outVertices.getKeys();
//        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
//          return true;
//      }
//
//    } catch (IOException e) {
//      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
//    }
//    return false;
//  }
//
//  public boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex, final PVertex.DIRECTION direction,
//      final String edgeType) {
//    if (toVertex == null)
//      throw new IllegalArgumentException("toVertex is null");
//
//    if (direction == null)
//      throw new IllegalArgumentException("Direction is null");
//
//    final PEdgeType type = getEdgeType(vertex, edgeType);
//
//    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(EDGES_INDEX_NAME);
//
//    try {
//      final Object[] keys = new Object[] { vertex.getIdentity(), (byte) direction.ordinal(), type.getDictionaryId(), toVertex };
//      final PIndexCursor outVertices = edgeIndex.iterator(keys);
//
//      return outVertices.hasNext();
//
//    } catch (IOException e) {
//      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
//    }
//  }
//
//  public PEdgeType getEdgeType(final PVertex vertex, final String edgeType) {
//    if (edgeType == null)
//      throw new IllegalArgumentException("Edge type is null");
//
//    final PDocumentType type = vertex.getDatabase().getSchema().getType(edgeType);
//    if (!(type instanceof PEdgeType))
//      throw new IllegalArgumentException("Type '" + edgeType + "' is not an edge type");
//
//    return (PEdgeType) type;
//  }
//
//  @Override
//  public void saveEdge(PModifiableEdge pModifiableEdge, final boolean creation) {
//    if (creation) {
//      // SET THE EDGE RID AS VALUE OF THE EDGE INDEX
//      final PIndex edgeIndex = pModifiableEdge.getDatabase().getSchema().getIndexByName(EDGES_INDEX_NAME);
//      final PEdgeType type = (PEdgeType) pModifiableEdge.getDatabase().getSchema().getType(pModifiableEdge.getType());
//      edgeIndex.put(new Object[] { pModifiableEdge.getOut(), (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(),
//          pModifiableEdge.getIn() }, pModifiableEdge.getIdentity());
//
//      // UPDATE OPPOSITE DIRECTION
//      final Object[] inKeys = new Object[] { pModifiableEdge.getIn(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId(),
//          pModifiableEdge.getOut() };
//      List<PRID> value = edgeIndex.get(inKeys);
//      if (!value.isEmpty())
//        edgeIndex.put(inKeys, pModifiableEdge.getIdentity());
//    }
//  }
//
//  private void setProperties(final PModifiableDocument edge, final Object[] properties) {
//    if (properties.length == 1 && properties[0] instanceof Map) {
//      // GET PROPERTIES FROM THE MAP
//      final Map<String, Object> map = (Map<String, Object>) properties[0];
//      for (Map.Entry<String, Object> entry : map.entrySet())
//        edge.set(entry.getKey(), entry.getValue());
//    } else {
//      if (properties.length % 2 != 0)
//        throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
//      for (int i = 0; i < properties.length; i += 2)
//        edge.set((String) properties[i], properties[i + 1]);
//    }
//  }
//}
