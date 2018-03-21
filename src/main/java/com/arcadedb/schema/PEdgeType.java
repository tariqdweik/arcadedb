package com.arcadedb.schema;

import com.arcadedb.graph.PEdge;

public class PEdgeType extends PDocumentType {
  private final int dictionaryId;

  public PEdgeType(final PSchemaImpl schema, final String name, final int dictionaryId) {
    super(schema, name);
    this.dictionaryId = dictionaryId;
  }

  public byte getType() {
    return PEdge.RECORD_TYPE;
  }

  public int getDictionaryId() {
    return dictionaryId;
  }
}
