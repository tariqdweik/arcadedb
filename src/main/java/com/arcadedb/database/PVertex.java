package com.arcadedb.database;

public class PVertex extends PModifiableDocument {
  public static final byte RECORD_TYPE = 1;

  public PVertex(final PDatabase graph, final PRID rid) {
    super(graph, rid);
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
