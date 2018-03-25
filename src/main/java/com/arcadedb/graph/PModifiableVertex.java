package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.utility.PLogManager;

import java.util.Iterator;

public class PModifiableVertex extends PModifiableDocument implements PVertexInternal {
  private static final int EDGES_LINKEDLIST_CHUNK_SIZE = 100;

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
    super(graph, typeName, rid);

    buffer.position(1); // SKIP RECORD TYPE
    outEdges = new PRID(graph, buffer.getInt(), buffer.getLong());
    inEdges = new PRID(graph, buffer.getInt(), buffer.getLong());

    init(buffer);
  }

  @Override
  public void onSerialize(final int bucketId) {
    if (getIdentity() == null) {
      final PModifiableEdgeChunk outChunk = new PModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
      database.createRecordNoLock(outChunk, database.getGraphEngine().getEdgesBucketName(database, bucketId, DIRECTION.OUT));
      outEdges = outChunk.getIdentity();

      final PModifiableEdgeChunk inChunk = new PModifiableEdgeChunk(database, EDGES_LINKEDLIST_CHUNK_SIZE);
      database.createRecordNoLock(inChunk, database.getGraphEngine().getEdgesBucketName(database, bucketId, DIRECTION.IN));
      inEdges = inChunk.getIdentity();
    }
  }

  public PRID getOutEdgesHeadChunk() {
    return outEdges;
  }

  public PRID getInEdgesHeadChunk() {
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final PRID outEdges) {
    if (outEdges.getBucketId() == 7 && outEdges.getPosition() == 2060476)
      PLogManager.instance().info(this, "setOutEdgesHeadChunk in %s!", rid);
    this.outEdges = outEdges;
  }

  @Override
  public void setInEdgesHeadChunk(final PRID inEdges) {
    if (inEdges.getBucketId() == 7 && inEdges.getPosition() == 2060476)
      PLogManager.instance().info(this, "setInEdgesHeadChunk in %s!", rid);
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
