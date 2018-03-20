package com.arcadedb.database;

public abstract class PBaseRecord implements PRecord {
  public static final byte RECORD_TYPE = 0;

  protected final PDatabase database;
  protected final String    typeName;
  protected       PRID      rid;

  protected PBaseRecord(final PDatabase database, final String typeName, final PRID rid) {
    this.database = database;
    this.typeName = typeName;
    this.rid = rid;
  }

  public String getType() {
    return typeName;
  }

  @Override
  public PRID getIdentity() {
    return rid;
  }

  @Override
  public PRecord getRecord() {
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof PRecord))
      return false;

    final PBaseRecord pRecord = (PBaseRecord) o;

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
