/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
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
