/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import com.arcadedb.exception.RecordNotFoundException;

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
  public Record getRecord(final boolean loadContent) {
    return this;
  }

  @Override
  public void reload() {
    if (rid != null && buffer == null && database.isOpen()) {
      try {
        buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
      } catch (RecordNotFoundException e) {
        // IGNORE IT
      }
    }
  }

  @Override
  public void delete() {
    database.deleteRecord(this);
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

  public void setBuffer(final Binary buffer) {
    this.buffer = buffer;
  }
}
