package com.arcadedb.database;

import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;

public class PRecordFactory {
  public PRecord newImmutableRecord(final PDatabase database, final String typeName, final PRID rid, final byte type) {
    switch (type) {
    case PBaseRecord.RECORD_TYPE:
      return new PImmutableDocument(database, typeName, rid, null);
    case PVertex.RECORD_TYPE:
      return new PImmutableVertex(database, typeName, rid, null);
    case PEdge.RECORD_TYPE:
      return new PImmutableEdge(database, typeName, rid, null);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public PRecord newImmutableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();
    content.reset();

    switch (type) {
    case PBaseRecord.RECORD_TYPE:
      return new PImmutableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PImmutableVertex(database, typeName, rid, content);
    case PEdge.RECORD_TYPE:
      return new PImmutableEdge(database, typeName, rid, content);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }

  public PRecord newModifiableRecord(final PDatabase database, final String typeName, final PRID rid, final PBinary content) {
    final byte type = content.getByte();
    content.reset();

    switch (type) {
    case PBaseRecord.RECORD_TYPE:
      return new PModifiableDocument(database, typeName, rid, content);
    case PVertex.RECORD_TYPE:
      return new PModifiableVertex(database, typeName, rid);
    case PEdge.RECORD_TYPE:
      return new PModifiableEdge(database, typeName, rid);
    }
    throw new PDatabaseMetadataException("Cannot find record type '" + type + "'");
  }
}
