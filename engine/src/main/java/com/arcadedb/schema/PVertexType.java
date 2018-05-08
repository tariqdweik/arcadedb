package com.arcadedb.schema;

import com.arcadedb.graph.Vertex;

public class PVertexType extends PDocumentType {

  public PVertexType(final PSchemaImpl schema, final String name) {
    super(schema, name);
  }

  public byte getType() {
    return Vertex.RECORD_TYPE;
  }
}
