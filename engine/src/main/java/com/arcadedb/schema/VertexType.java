/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

import com.arcadedb.graph.Vertex;

public class VertexType extends DocumentType {

  public VertexType(final SchemaImpl schema, final String name) {
    super(schema, name);
  }

  public byte getType() {
    return Vertex.RECORD_TYPE;
  }
}
