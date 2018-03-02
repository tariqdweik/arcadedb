package com.arcadedb.database;

/**
 * Immutable class.
 */
public class PRID {
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
}
