package com.arcadedb.database;

public interface BucketSelectionStrategy {
  void setTotalBuckets(int total);

  int getBucketToSave();

  String getName();
}
