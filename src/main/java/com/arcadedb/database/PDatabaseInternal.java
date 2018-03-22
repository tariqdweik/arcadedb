package com.arcadedb.database;

public interface PDatabaseInternal {
  void saveRecord(PModifiableDocument record);

  void saveRecord(PRecord record, String bucketName);
}
