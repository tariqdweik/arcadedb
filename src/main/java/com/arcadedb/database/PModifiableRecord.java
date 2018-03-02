package com.arcadedb.database;

public interface PModifiableRecord extends PRecord {
  void set(String name, final Object value);

  void save();

  void save(String bucketName);
}
