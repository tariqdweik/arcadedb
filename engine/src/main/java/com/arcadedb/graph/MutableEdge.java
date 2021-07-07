/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.serializer.BinaryTypes;

/**
 * Mutable edge that supports updates. After any changes, call the method {@link #save()} to mark the record as dirty in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableEdge
 */
public class MutableEdge extends MutableDocument implements Edge {
  private RID out;
  private RID in;

  public MutableEdge(final Database graph, final String typeName, final RID out, RID in) {
    super(graph, typeName, null);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final String typeName, final RID edgeRID, final RID out, RID in) {
    super(graph, typeName, edgeRID);
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
  public MutableEdge set(final Object... properties) {
    super.set(properties);
    checkForUpgradeLightWeigth();
    return this;
  }

  @Override
  public MutableEdge set(final String name, final Object value) {
    super.set(name, value);
    checkForUpgradeLightWeigth();
    return this;
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

  private void checkForUpgradeLightWeigth() {
    if (rid != null && rid.getPosition() < 0) {
      // REMOVE THE TEMPORARY RID SO IT WILL BE CREATED AT SAVE TIME
      rid = null;

      save();

      // UPDATE BOTH REFERENCES WITH THE NEW RID
      database.getGraphEngine().connectEdge(database, (VertexInternal) out.getVertex(true), in, this, true);
    }
  }
}
