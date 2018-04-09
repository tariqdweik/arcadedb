package com.arcadedb.graph;

import com.arcadedb.database.*;

import java.util.Iterator;

public class PModifiableVertex extends PModifiableDocument implements PVertexInternal {
  private PRID outEdges;
  private PRID inEdges;

  /**
   * Creation constructor.
   */
  public PModifiableVertex(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  /**
   * Copy constructor from PImmutableVertex.modify().
   */
  public PModifiableVertex(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);

    buffer.position(1); // SKIP RECORD TYPE
    outEdges = new PRID(graph, buffer.getInt(), buffer.getLong());
    if (outEdges.getBucketId() == -1)
      outEdges = null;
    inEdges = new PRID(graph, buffer.getInt(), buffer.getLong());
    if (inEdges.getBucketId() == -1)
      inEdges = null;

    this.propertiesStartingPosition = propertiesStartingPosition;
    }

  public PRID getOutEdgesHeadChunk() {
    return outEdges;
  }

  public PRID getInEdgesHeadChunk() {
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final PRID outEdges) {
    this.outEdges = outEdges;
  }

  @Override
  public void setInEdgesHeadChunk(final PRID inEdges) {
    this.inEdges = inEdges;
  }

  @Override
  public byte getRecordType() {
    return PVertex.RECORD_TYPE;
  }

  public PEdge newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    return database.getGraphEngine().newEdge(this, edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public long countEdges(DIRECTION direction, String edgeType) {
    return database.getGraphEngine().countEdges(this, direction, edgeType);
  }

  @Override
  public Iterator<PEdge> getEdges() {
    return database.getGraphEngine().getEdges(this);
  }

  @Override
  public Iterator<PEdge> getEdges(final DIRECTION direction) {
    return database.getGraphEngine().getEdges(this, direction);
  }

  @Override
  public Iterator<PEdge> getEdges(final DIRECTION direction, final String edgeType) {
    return database.getGraphEngine().getEdges(this, direction, edgeType);
  }

  @Override
  public Iterator<PVertex> getVertices() {
    return database.getGraphEngine().getVertices(this);
  }

  @Override
  public Iterator<PVertex> getVertices(final DIRECTION direction) {
    return database.getGraphEngine().getVertices(this, direction);
  }

  @Override
  public Iterator<PVertex> getVertices(final DIRECTION direction, final String edgeType) {
    return database.getGraphEngine().getVertices(this, direction, edgeType);
  }

  @Override
  public boolean isConnectedTo(final PIdentifiable toVertex) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex);
  }

  @Override
  public boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex, direction);
  }
}
