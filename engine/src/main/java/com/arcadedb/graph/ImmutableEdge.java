/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;

public class ImmutableEdge extends ImmutableDocument implements Edge {
  private RID out;
  private RID in;

  public ImmutableEdge(final Database graph, final String typeName, final RID edgeRID, final RID out, RID in) {
    super(graph, typeName, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public ImmutableEdge(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      out = new RID(graph, buffer.getInt(), buffer.getLong());
      in = new RID(graph, buffer.getInt(), buffer.getLong());
      propertiesStartingPosition = buffer.position();
    }
  }

  public MutableEdge modify() {
    checkForLazyLoading();
    return new MutableEdge(database, typeName, rid, buffer.copy());
  }

  @Override
  public RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    checkForLazyLoading();
    return (Vertex) out.getRecord();
  }

  @Override
  public RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public Vertex getInVertex() {
    checkForLazyLoading();
    return (Vertex) in.getRecord();
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
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
    if (super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      out = new RID(database, buffer.getInt(), buffer.getLong());
      in = new RID(database, buffer.getInt(), buffer.getLong());
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }
}
