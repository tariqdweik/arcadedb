package com.arcadedb.database;

public class PEdge extends PModifiableDocument {
  public static final byte RECORD_TYPE = 2;

  private PIdentifiable out;
  private PIdentifiable in;

  public PEdge(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  public PIdentifiable getOut() {
    return out;
  }

  public PIdentifiable getIn() {
    return in;
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
