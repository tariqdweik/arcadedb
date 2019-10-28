/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public interface BucketSelectionStrategy {
  void setTotalBuckets(int total);

  int getBucketToSave(boolean async);

  String getName();
}
