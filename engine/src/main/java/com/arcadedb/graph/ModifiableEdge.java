package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.RID;

public class ModifiableEdge extends ModifiableDocument implements Edge {
  private RID out;
  private RID in;

  public ModifiableEdge(final Database graph, final String typeName, final RID out, RID in) {
    super(graph, typeName, null);
    this.out = out;
    this.in = in;
  }

  public ModifiableEdge(final Database graph, final String typeName, final RID rid) {
    super(graph, typeName, rid);
  }

  public ModifiableEdge(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
    this.out = new RID(graph, buffer.getInt(), buffer.getLong());
    this.in = new RID(graph, buffer.getInt(), buffer.getLong());
    this.propertiesStartingPosition = buffer.position();
  }

  @Override
  public RID getOut() {
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    return (Vertex) out.getRecord();
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return (Vertex) in.getRecord();
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) out.getRecord();
    else
      return (Vertex) in.getRecord();
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }
}
