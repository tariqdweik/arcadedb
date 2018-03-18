package com.arcadedb.database;

/**
 * Immutable class.
 */
public class PRID implements PIdentifiable, Comparable<PRID> {
  private int  bucketId;
  private long offset;

  public PRID(final int bucketId, final long offset) {
    this.bucketId = bucketId;
    this.offset = offset;
  }

  public int getBucketId() {
    return bucketId;
  }

  public long getPosition() {
    return offset;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(12);
    buffer.append('#');
    buffer.append(bucketId);
    buffer.append(':');
    buffer.append(offset);
    return buffer.toString();
  }

  @Override
  public PRID getIdentity() {
    return this;
  }

  @Override
  public int compareTo(final PRID o) {
    if (bucketId > o.bucketId)
      return 1;
    else if (bucketId < o.bucketId)
      return -1;

    if (offset > o.offset)
      return 1;
    else if (offset < o.offset)
      return -1;

    return 0;
  }
}
