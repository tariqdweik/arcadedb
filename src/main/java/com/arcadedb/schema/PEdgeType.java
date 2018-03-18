package com.arcadedb.schema;

public class PEdgeType extends PDocumentType {
  private final int dictionaryId;

  public PEdgeType(final PSchemaImpl schema, final String name, final int dictionaryId) {
    super(schema, name);
    this.dictionaryId = dictionaryId;
  }

  public int getDictionaryId() {
    return dictionaryId;
  }
}
