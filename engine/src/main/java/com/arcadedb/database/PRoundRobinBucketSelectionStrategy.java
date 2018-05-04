package com.arcadedb.database;

public class PRoundRobinBucketSelectionStrategy implements PBucketSelectionStrategy {
  private int current = -1;
  private int total;

  @Override
  public void setTotalBuckets(final int total) {
    this.total = total;
    if (current >= total)
      // RESET IT
      current = -1;
  }

  @Override
  public int getBucketToSave() {
    if (++current >= total)
      current = 0;
    return current;
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
