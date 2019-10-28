/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
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
