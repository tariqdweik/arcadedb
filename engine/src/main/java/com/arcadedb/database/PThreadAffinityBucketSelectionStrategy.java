package com.arcadedb.database;

public class PThreadAffinityBucketSelectionStrategy implements PBucketSelectionStrategy {
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
