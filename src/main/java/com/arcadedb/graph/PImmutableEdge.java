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

  public PImmutableEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      out = new PRID(graph, buffer.getInt(), buffer.getLong());
      in = new PRID(graph, buffer.getInt(), buffer.getLong());
      propertiesStartingPosition = buffer.position();
    }
  }

  public PModifiableEdge modify() {
    return new PModifiableEdge(database, typeName, rid, buffer);
  }

  @Override
  public PRID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public PRID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public byte getRecordType() {
    return PEdge.RECORD_TYPE;
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      out = new PRID(database, buffer.getInt(), buffer.getLong());
      in = new PRID(database, buffer.getInt(), buffer.getLong());
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }
}
