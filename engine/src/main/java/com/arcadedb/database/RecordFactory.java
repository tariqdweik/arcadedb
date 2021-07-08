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

package com.arcadedb.database;

import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.graph.*;

public class RecordFactory {
  public Record newImmutableRecord(final Database database, final String typeName, final RID rid, final byte type) {
    switch (type) {
    case Document.RECORD_TYPE:
      return new ImmutableDocument(database, typeName, rid, null);
    case Vertex.RECORD_TYPE:
      return new ImmutableVertex(database, typeName, rid, null);
    case Edge.RECORD_TYPE:
      return new ImmutableEdge(database, typeName, rid, null);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid, null);
    case EmbeddedDocument.RECORD_TYPE:
      return new ImmutableEmbeddedDocument(database, typeName, null);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public Record newImmutableRecord(final Database database, final String typeName, final RID rid, final Binary content) {
    final byte type = content.getByte();

    switch (type) {
    case Document.RECORD_TYPE:
      return new ImmutableDocument(database, typeName, rid, content);
    case Vertex.RECORD_TYPE:
      return new ImmutableVertex(database, typeName, rid, content);
    case Edge.RECORD_TYPE:
      return new ImmutableEdge(database, typeName, rid, content);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid, content);
    case EmbeddedDocument.RECORD_TYPE:
      return new ImmutableEmbeddedDocument(database, typeName, content);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public Record newModifiableRecord(final Database database, final String typeName, final RID rid, final Binary content) {
    final byte type = content.getByte();

    switch (type) {
    case Document.RECORD_TYPE:
      return new MutableDocument(database, typeName, rid, content);
    case Vertex.RECORD_TYPE:
      return new MutableVertex(database, typeName, rid);
    case Edge.RECORD_TYPE:
      return new MutableEdge(database, typeName, rid);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid);
    case EmbeddedDocument.RECORD_TYPE:
      return new MutableEmbeddedDocument(database, typeName, content);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + type + "'");
  }
}
