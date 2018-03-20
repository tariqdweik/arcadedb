package com.arcadedb.database;

import com.arcadedb.database.graph.PEdge;
import com.arcadedb.database.graph.PModifiableEdge;

public class PImmutableEdge extends PImmutableDocument implements PEdge {
  private PIdentifiable out;
  private PIdentifiable in;

  public PImmutableEdge(PDatabase graph, String typeName, PRID rid, PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  public PModifiableEdge modify() {
    return new PModifiableEdge(database, typeName, rid, buffer);
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
