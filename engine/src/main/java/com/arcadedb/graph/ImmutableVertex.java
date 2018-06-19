/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;

import java.util.Map;

public class ImmutableVertex extends ImmutableDocument implements VertexInternal {
  private RID outEdges;
  private RID inEdges;

  public ImmutableVertex(final Database database, final String typeName, final RID rid, final Binary buffer) {
    super(database, typeName, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  public ModifiableVertex modify() {
    checkForLazyLoading();
    return new ModifiableVertex(database, typeName, rid, buffer.copy());
  }

  @Override
  public Object get(final String name) {
    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer().deserializeProperties(database, buffer, name);
    return map.get(name);
  }

  @Override
  public RID getOutEdgesHeadChunk() {
    checkForLazyLoading();
    return outEdges;
  }

  @Override
  public RID getInEdgesHeadChunk() {
    checkForLazyLoading();
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    throw new UnsupportedOperationException("setOutEdgesHeadChunk");
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    throw new UnsupportedOperationException("setInEdgesHeadChunk");
  }

  public Edge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    return database.getGraphEngine().newEdge(this, edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public long countEdges(DIRECTION direction, String edgeType) {
    return database.getGraphEngine().countEdges(this, direction, edgeType);
  }

  @Override
  public Iterable<Edge> getEdges() {
    return database.getGraphEngine().getEdges(this);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction) {
    return database.getGraphEngine().getEdges(this, direction);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String[] edgeTypes) {
    return database.getGraphEngine().getEdges(this, direction, edgeTypes);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return database.getGraphEngine().getVertices(this);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction) {
    return database.getGraphEngine().getVertices(this, direction);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String[] edgeTypes) {
    return database.getGraphEngine().getVertices(this, direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex, direction);
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }
}
