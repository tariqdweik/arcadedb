package com.arcadedb.graph;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PImmutableDocument;
import com.arcadedb.database.PRID;

public class PImmutableEdge extends PImmutableDocument implements PEdge {
  private PRID out;
  private PRID in;

  public PImmutableEdge(final PDatabase graph, final String typeName, final PRID edgeRID, final PRID out, PRID in) {
    super(graph, typeName, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public PImmutableEdge(PDatabase graph, String typeName, PRID rid, PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  public PModifiableEdge modify() {
    return new PModifiableEdge(database, typeName, rid, buffer, out, in);
  }

  @Override
  public PRID getOut() {
    return out;
  }

  @Override
  public PRID getIn() {
    return in;
  }

  @Override
  public byte getRecordType() {
    return PEdge.RECORD_TYPE;
  }
}
