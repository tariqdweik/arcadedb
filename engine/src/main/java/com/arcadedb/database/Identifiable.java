package com.arcadedb.database;

public interface Identifiable {
  RID getIdentity();

  Record getRecord();
}
