package com.arcadedb.graph;

import com.arcadedb.database.*;

import java.util.Iterator;
import java.util.Map;

public class PImmutableVertex extends PImmutableDocument implements PVertexInternal {
  private PRID outEdges;
  private PRID inEdges;

  public PImmutableVertex(final PDatabase database, final String typeName, final PRID rid, final PBinary buffer) {
    super(database, typeName, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new PRID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new PRID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public byte getRecordType() {
    return PVertex.RECORD_TYPE;
  }

  public PModifiableVertex modify() {
    checkForLazyLoading();
    // CREATE A SEPARATE OBJECT THAT POINTS TO THE SAME BUFFER TO AVOID CONCURRENCY ON THE BUFFER POSITION
    return new PModifiableVertex(database, typeName, rid, buffer.slice());
  }

  @Override
  public Object get(final String name) {
    checkForLazyLoading();
    buffer.position(propertiesStartingPosition);
    final Map<String, Object> map = database.getSerializer().deserializeProperties(database, buffer, name);
    return map.get(name);
  }

  @Override
  public PRID getOutEdgesHeadChunk() {
    checkForLazyLoading();
    return outEdges;
  }

  @Override
  public PRID getInEdgesHeadChunk() {
    checkForLazyLoading();
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final PRID outEdges) {
    throw new UnsupportedOperationException("setOutEdgesHeadChunk");
  }

  @Override
  public void setInEdgesHeadChunk(final PRID inEdges) {
    throw new UnsupportedOperationException("setOutEdgesHeadChunk");
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

  @Override
  protected boolean checkForLazyLoading() {
    if (super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new PRID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new PRID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }
}
