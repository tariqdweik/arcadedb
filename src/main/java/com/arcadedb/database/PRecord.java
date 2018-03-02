package com.arcadedb.database;

import java.util.Set;

public interface PRecord {
  Object get(String name);

  PRID getIdentity();

  byte getRecordType();

  Set<String> getPropertyNames();

  PDatabase getDatabase();
}
