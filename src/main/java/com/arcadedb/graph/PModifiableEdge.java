package com.arcadedb.graph;

import com.arcadedb.database.*;

public class PModifiableEdge extends PModifiableDocument implements PEdge {
  private PIdentifiable out;
  private PIdentifiable in;

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public PIdentifiable getOut() {
    return out;
  }

  @Override
  public PIdentifiable getIn() {
    return in;
  }

  @Override
  public byte getRecordType() {
    return PEdge.RECORD_TYPE;
  }
}
