package com.arcadedb.database;

import java.util.Set;

public interface PRecord extends PIdentifiable {
  Object get(String name);

  PRID getIdentity();

  String getType();

  byte getRecordType();

  Set<String> getPropertyNames();

  PDatabase getDatabase();
}
