package com.arcadedb.database;

public abstract class PBaseDocument implements PRecord {
  public static final byte RECORD_TYPE = 0;

  protected final PDatabase database;
  protected       String    typeName;
  protected       PRID      rid;

  protected PBaseDocument(final PDatabase database, final PRID rid) {
    this.database = database;
    this.rid = rid;
  }

  public String getType() {
    return typeName;
  }

  public void setType(final String typeName) {
    this.typeName = typeName;
  }

  @Override
  public PRID getIdentity() {
    return rid;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof PRecord))
      return false;

    final PBaseDocument pRecord = (PBaseDocument) o;

    if (!database.equals(pRecord.database))
      return false;
    return rid != null ? rid.equals(pRecord.rid) : pRecord.rid == null;
  }

  @Override
  public int hashCode() {
    int result = database.hashCode();
    result = 31 * result + (rid != null ? rid.hashCode() : 0);
    return result;
  }

  @Override
  public PDatabase getDatabase() {
    return database;
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
