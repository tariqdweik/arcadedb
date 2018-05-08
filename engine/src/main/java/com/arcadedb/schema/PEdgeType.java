/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

import com.arcadedb.graph.Edge;

public class PEdgeType extends PDocumentType {
  public PEdgeType(final PSchemaImpl schema, final String name) {
    super(schema, name);
  }

  public byte getType() {
    return Edge.RECORD_TYPE;
  }
}
