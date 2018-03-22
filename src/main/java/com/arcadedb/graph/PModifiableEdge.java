package com.arcadedb.graph;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRID;
import com.arcadedb.index.PIndex;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PSchemaImpl;

import java.util.List;

public class PModifiableEdge extends PModifiableDocument implements PEdge {
  private PRID out;
  private PRID in;

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID out, PRID in) {
    super(graph, typeName, null);
    this.out = out;
    this.in = in;
  }

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  public PModifiableEdge(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer, final PRID out,
      final PRID in) {
    super(graph, typeName, rid, buffer);
    this.out = out;
    this.in = in;
  }

  @Override
  public void save() {
    final boolean noIdentity = rid == null;

    super.save();

    if (noIdentity) {
      // SET THE EDGE RID AS VALUE OF THE EDGE INDEX
      final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);
      final PEdgeType type = (PEdgeType) database.getSchema().getType(typeName);
      edgeIndex.put(new Object[] { out, (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(), in }, rid);

      // UPDATE OPPOSITE DIRECTION
      final Object[] inKeys = new Object[] { out, (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(), in };
      List<PRID> value = edgeIndex.get(inKeys);
      if (!value.isEmpty())
        edgeIndex.put(inKeys, rid);
    }
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
