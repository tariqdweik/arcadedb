/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

public interface BucketSelectionStrategy {
  void setTotalBuckets(int total);

  int getBucketToSave(boolean async);

  String getName();
}
