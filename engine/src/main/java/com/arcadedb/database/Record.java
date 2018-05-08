package com.arcadedb.database;

public interface Record extends Identifiable {
  RID getIdentity();

  byte getRecordType();

  Database getDatabase();

  Record modify();
}
