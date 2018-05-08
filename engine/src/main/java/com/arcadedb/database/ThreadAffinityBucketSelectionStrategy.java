/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public class ThreadAffinityBucketSelectionStrategy implements BucketSelectionStrategy {
  private int total;

  @Override
  public void setTotalBuckets(final int total) {
    this.total = total;
  }

  @Override
  public int getBucketToSave() {
    return (int) (Thread.currentThread().getId() % total);
  }

  @Override
  public String getName() {
    return "thread-affinity";
  }

  @Override
  public String toString() {
    return getName();
  }
}
