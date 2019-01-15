/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryTypes;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Map;

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
  public Object get(final String name) {
    if (isLightweight())
      return null;
    return super.get(name);
  }

  public MutableEdge modify() {
    checkForLazyLoading();
    if (isLightweight())
      // TODO: TRANSFORM LIGHTWEIGHT EDGE INTO REGULAR ONE AND UPDATE LINKS IN OUT/IN VERTICES
      throw new IllegalStateException("Lightweight edges cannot be modified");

    if (buffer != null) {
      buffer.rewind();
      return new MutableEdge(database, typeName, rid, buffer.copy());
    }
    return new MutableEdge(database, typeName, rid, getOut(), getIn());
  }

  @Override
  public RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    checkForLazyLoading();
    return out.getVertex();
  }

  @Override
  public RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public Vertex getInVertex() {
    checkForLazyLoading();
    return in.getVertex();
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
    if (rid != null && !isLightweight() && super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    if (isLightweight())
      return Collections.emptyMap();
    return super.toMap();
  }

  @Override
  public JSONObject toJSON() {
    if (isLightweight())
      return new JSONObject();
    return super.toJSON();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(out.toString());
    buffer.append("<->");
    buffer.append(in.toString());

    if (!isLightweight()) {
      buffer.append("(");
      buffer.append(super.toString());
      buffer.append(")");
    }

    return buffer.toString();
  }

  @Override
  public boolean isLightweight() {
    return rid.getPosition() < 0;
  }
}
