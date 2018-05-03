package com.arcadedb.database;

public abstract class PBaseRecord implements PRecord {
  protected final PDatabaseInternal database;
  protected       PRID              rid;
  protected       PBinary           buffer;

  protected PBaseRecord(final PDatabase database, final PRID rid, final PBinary buffer) {
    this.database = (PDatabaseInternal) database;
    this.rid = rid;
    this.buffer = buffer;
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
    if (o == null || !(o instanceof PIdentifiable))
      return false;

    final PRID pRID = ((PIdentifiable) o).getIdentity();

    return rid != null ? rid.equals(pRID) : pRID == null;
  }

  @Override
  public int hashCode() {
    int result = database.hashCode();
    result = 31 * result + (rid != null ? rid.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return rid != null ? rid.toString() : "#?:?";
  }

  @Override
  public PDatabase getDatabase() {
    return database;
  }

  public PBinary getBuffer() {
    return buffer;
  }

}
