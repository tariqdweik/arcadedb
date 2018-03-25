package com.arcadedb.database;

public abstract class PBaseRecord implements PRecord {
  protected final PDatabaseInternal database;
  protected       PRID              rid;

  protected PBaseRecord(final PDatabase database, final PRID rid) {
    this.database = (PDatabaseInternal) database;
    this.rid = rid;
  }

  @Override
  public PRID getIdentity() {
    return rid;
  }

  @Override
  public PRecord getRecord() {
    return this;
  }

  public void onSerialize(int bucketId) {
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
}
