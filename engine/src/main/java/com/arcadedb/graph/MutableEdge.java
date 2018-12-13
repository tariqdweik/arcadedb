/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryTypes;

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

  public MutableEdge modify() {
    return this;
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
    return out.getVertex();
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return in.getVertex();
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return out.getVertex();
    else
      return in.getVertex();
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  public MutableEdge save() {
    if (getIdentity() != null && getIdentity().getPosition() < 0)
      // LIGHTWEIGHT
      return this;

    return (MutableEdge) super.save();
  }

  @Override
  public MutableEdge save(final String bucketName) {
    if (getIdentity() != null && getIdentity().getPosition() < 0)
      // LIGHTWEIGHT
      return this;

    return (MutableEdge) super.save(bucketName);
  }

  private void init() {
    if (buffer != null) {
      buffer.position(1);
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      this.propertiesStartingPosition = buffer.position();
    }
  }
}
