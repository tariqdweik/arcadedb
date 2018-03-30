package com.arcadedb.database;

public abstract class PBaseDocument extends PBaseRecord implements PDocument {
  protected final String  typeName;
  protected int propertiesStartingPosition = 1;

  protected PBaseDocument(final PDatabase database, final String typeName, final PRID rid, final PBinary buffer) {
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
