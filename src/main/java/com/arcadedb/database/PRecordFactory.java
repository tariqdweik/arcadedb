package com.arcadedb.database;

import com.arcadedb.exception.PDatabaseMetadataException;

public class PRecordFactory {
  public PRecord newImmutableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();
    content.reset();

    switch (type) {
    case PImmutableDocument.RECORD_TYPE:
      return new PImmutableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PVertex(database, typeName, rid, content);
    case PEdge.RECORD_TYPE:
      return new PEdge(database, typeName, rid, content);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public PRecord newModifiableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();
    content.reset();

    switch (type) {
    case PModifiableDocument.RECORD_TYPE:
      return new PModifiableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PVertex(database, typeName, rid);
    case PEdge.RECORD_TYPE:
      return new PEdge(database, typeName, rid);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }
}
