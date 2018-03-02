package com.arcadedb.database;

public interface PBucketSelectionStrategy {
  void setTotalBuckets(int total);

  int getBucketToSave();

  String getName();
}
