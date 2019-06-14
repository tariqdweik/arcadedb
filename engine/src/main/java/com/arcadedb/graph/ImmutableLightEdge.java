/*
 * Copyright (c) 2019 - Arcade Analytics LTD (https://arcadeanalytics.com)
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

public class ImmutableLightEdge extends ImmutableDocument implements LightEdge {
  private RID out;
  private RID in;

  public ImmutableLightEdge(final Database graph, final String typeName, final RID edgeRID, final RID out, final RID in) {
    super(graph, typeName, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public ImmutableLightEdge(final Database graph, final String typeName, final Binary buffer) {
    super(graph, typeName, null, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      in = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID);
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public Object get(final String propertyName) {
    return null;
  }

  public MutableEdge modify() {
    throw new IllegalStateException("Lightweight edges cannot be modified");
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
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    return Collections.emptyMap();
  }

  @Override
  public JSONObject toJSON() {
    return new JSONObject();
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
