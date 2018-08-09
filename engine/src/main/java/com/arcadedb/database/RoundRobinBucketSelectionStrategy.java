/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public class RoundRobinBucketSelectionStrategy implements BucketSelectionStrategy {
  private volatile int current = -1;
  private          int total;

  @Override
  public void setTotalBuckets(final int total) {
    this.total = total;
    if (current >= total)
      // RESET IT
      current = -1;
  }

  @Override
  public int getBucketToSave() {
    // COPY THE VALUE ON THE HEAP FOR MULTI-THREAD ACCESS
    int bucketIndex = ++current;
    if (bucketIndex >= total) {
      current = 0;
      bucketIndex = 0;
    }
    return bucketIndex;
  }

  @Override
  public String getName() {
    return "round-robin";
  }

  @Override
  public String toString() {
    return getName();
  }
}
