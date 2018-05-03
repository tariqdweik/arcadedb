package com.arcadedb.database;

import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.graph.*;

public class PRecordFactory {
  public PRecord newImmutableRecord(final PDatabase database, final String typeName, final PRID rid, final byte type) {
    switch (type) {
    case PDocument.RECORD_TYPE:
      return new PImmutableDocument(database, typeName, rid, null);
    case PVertex.RECORD_TYPE:
      return new PImmutableVertex(database, typeName, rid, null);
    case PEdge.RECORD_TYPE:
      return new PImmutableEdge(database, typeName, rid, (PBinary) null);
    case PEdgeChunk.RECORD_TYPE:
      return new PModifiableEdgeChunk(database, rid, (PBinary) null);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public PRecord newImmutableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();

    switch (type) {
    case PDocument.RECORD_TYPE:
      return new PImmutableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PImmutableVertex(database, typeName, rid, content);
    case PEdge.RECORD_TYPE:
      return new PImmutableEdge(database, typeName, rid, content);
    case PEdgeChunk.RECORD_TYPE:
      return new PModifiableEdgeChunk(database, rid, content);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public PRecord newModifiableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();

    switch (type) {
    case PDocument.RECORD_TYPE:
      return new PModifiableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PModifiableVertex(database, typeName, rid);
    case PEdge.RECORD_TYPE:
      return new PModifiableEdge(database, typeName, rid);
    case PEdgeChunk.RECORD_TYPE:
      return new PModifiableEdgeChunk(database, rid);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }
}
