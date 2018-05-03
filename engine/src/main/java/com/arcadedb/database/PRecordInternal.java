package com.arcadedb.database;

public interface PRecordInternal {
  void setIdentity(PRID rid);

  PBinary getBuffer();
}
