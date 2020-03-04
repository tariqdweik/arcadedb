/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.serializer.BinaryTypes;

public class ImmutableEdge extends ImmutableDocument implements Edge {
  private RID out;
  private RID in;

  public ImmutableEdge(final Database graph, final String typeName, final RID edgeRID, final RID out, final RID in) {
    super(graph, typeName, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public ImmutableEdge(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      in = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public Object get(final String propertyName) {
    return super.get(propertyName);
  }

  public synchronized MutableEdge modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null && recordInCache != this && recordInCache instanceof MutableEdge)
      return (MutableEdge) recordInCache;

    checkForLazyLoading();
    if (buffer != null) {
      buffer.rewind();
      return new MutableEdge(database, typeName, rid, buffer.copy());
    }
    return new MutableEdge(database, typeName, rid, getOut(), getIn());
  }

  @Override
  public synchronized RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public synchronized Vertex getOutVertex() {
    checkForLazyLoading();
    return out.getVertex();
  }

  @Override
  public synchronized RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public synchronized Vertex getInVertex() {
    checkForLazyLoading();
    return in.getVertex();
  }

  @Override
  public synchronized Vertex getVertex(final Vertex.DIRECTION iDirection) {
    checkForLazyLoading();
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) out.getRecord();
    else
      return (Vertex) in.getRecord();
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (rid != null && super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(out.toString());
    buffer.append("<->");
    buffer.append(in.toString());
    return buffer.toString();
  }
}
