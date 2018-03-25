package com.arcadedb.database;

public interface PRecord extends PIdentifiable {
  PRID getIdentity();

  byte getRecordType();

  PDatabase getDatabase();

  PRecord modify();
}
