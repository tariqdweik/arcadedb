package com.arcadedb.database;

public class PEdge extends PModifiableDocument {
  public static final byte RECORD_TYPE = 2;

  public PEdge(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
