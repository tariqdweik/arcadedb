/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.schema;

import com.arcadedb.graph.Edge;

public class EdgeType extends DocumentType {
  public EdgeType(final SchemaImpl schema, final String name) {
    super(schema, name);
  }

  public byte getType() {
    return Edge.RECORD_TYPE;
  }
}
