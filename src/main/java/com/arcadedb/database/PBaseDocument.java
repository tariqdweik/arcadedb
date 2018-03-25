package com.arcadedb.database;

public abstract class PBaseDocument extends PBaseRecord implements PDocument {
  protected final String typeName;

  protected PBaseDocument(final PDatabase database, final String typeName, final PRID rid) {
    super(database, rid);
    this.typeName = typeName;
  }

  public String getType() {
    return typeName;
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
