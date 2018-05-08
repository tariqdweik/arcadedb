package com.arcadedb.database;

public interface RecordInternal {
  void setIdentity(RID rid);

  Binary getBuffer();
}
