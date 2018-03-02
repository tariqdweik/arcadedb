package com.arcadedb.database;

public class PEdge extends PModifiableDocument {
  public static final byte RECORD_TYPE = 2;

  public PEdge(final PDatabase graph, final PRID rid) {
    super(graph, rid);
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
