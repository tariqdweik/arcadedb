package com.arcadedb.schema;

import com.arcadedb.graph.PEdge;

public class PEdgeType extends PDocumentType {
  public PEdgeType(final PSchemaImpl schema, final String name) {
    super(schema, name);
  }

  public byte getType() {
    return PEdge.RECORD_TYPE;
  }
}
