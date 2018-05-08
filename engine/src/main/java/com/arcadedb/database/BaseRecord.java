package com.arcadedb.database;

public abstract class BaseRecord implements Record {
  protected final DatabaseInternal database;
  protected       RID              rid;
  protected       Binary           buffer;

  protected BaseRecord(final Database database, final RID rid, final Binary buffer) {
    this.database = (DatabaseInternal) database;
    this.rid = rid;
    this.buffer = buffer;
  }

  @Override
  public RID getIdentity() {
    return rid;
  }

  @Override
  public Record getRecord() {
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof Identifiable))
      return false;

    final RID pRID = ((Identifiable) o).getIdentity();

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
  public Database getDatabase() {
    return database;
  }

  public Binary getBuffer() {
    return buffer;
  }

}
