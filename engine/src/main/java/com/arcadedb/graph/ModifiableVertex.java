/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;

public class ModifiableVertex extends ModifiableDocument implements VertexInternal {
  private RID outEdges;
  private RID inEdges;

  /**
   * Creation constructor.
   */
  public ModifiableVertex(final Database graph, final String typeName, final RID rid) {
    super(graph, typeName, rid);
  }

  /**
   * Copy constructor from ImmutableVertex.modify().
   */
  public ModifiableVertex(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
    init();
  }

  @Override
  public ModifiableVertex save() {
    return (ModifiableVertex) super.save();
  }

  @Override
  public ModifiableVertex save(final String bucketName) {
    return (ModifiableVertex) super.save(bucketName);
  }

  @Override
  public void reload() {
    super.reload();
    init();
  }

  @Override
  public void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    init();
  }

  public RID getOutEdgesHeadChunk() {
    return outEdges;
  }

  public RID getInEdgesHeadChunk() {
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    this.outEdges = outEdges;
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    this.inEdges = inEdges;
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  public ModifiableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional, final Object... properties) {
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

  private void init() {
    if (buffer != null) {
      buffer.position(1);
      this.outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (this.outEdges.getBucketId() == -1)
        this.outEdges = null;
      this.inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (this.inEdges.getBucketId() == -1)
        this.inEdges = null;

      this.propertiesStartingPosition = buffer.position();
    }
  }
}
