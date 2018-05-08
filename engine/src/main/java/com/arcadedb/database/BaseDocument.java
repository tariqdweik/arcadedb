package com.arcadedb.database;

public abstract class BaseDocument extends BaseRecord implements Document {
  protected final String typeName;
  protected       int    propertiesStartingPosition = 1;

  protected BaseDocument(final Database database, final String typeName, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
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
