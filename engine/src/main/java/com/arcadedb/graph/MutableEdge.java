/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;

public class MutableEdge extends MutableDocument implements Edge {
  private RID out;
  private RID in;

  public MutableEdge(final Database graph, final String typeName, final RID out, RID in) {
    super(graph, typeName, null);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final String typeName, final RID rid) {
    super(graph, typeName, rid);
  }

  public MutableEdge(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
    init();
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

  @Override
  public RID getOut() {
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    return (Vertex) out.getRecord();
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return (Vertex) in.getRecord();
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) out.getRecord();
    else
      return (Vertex) in.getRecord();
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  private void init() {
    if (buffer != null) {
      buffer.position(1);
      this.out = new RID(database, buffer.getInt(), buffer.getLong());
      this.in = new RID(database, buffer.getInt(), buffer.getLong());
      this.propertiesStartingPosition = buffer.position();
    }
  }
}
