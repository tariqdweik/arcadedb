package com.arcadedb.database;

public interface PIdentifiable {
  PRID getIdentity();

  PRecord getRecord();
}
