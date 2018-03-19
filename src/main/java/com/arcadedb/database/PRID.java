package com.arcadedb.database;

/**
 * Immutable class.
 */
public class PRID implements PIdentifiable, Comparable<PIdentifiable> {
  private final PDatabase database;
  private       int       bucketId;
  private       long      offset;

  public PRID(final PDatabase database, final int bucketId, final long offset) {
    this.database = database;
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
  public PRecord getRecord() {
    return database.lookupByRID(this);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof PIdentifiable))
      return false;

    final PRID o = ((PIdentifiable) obj).getIdentity();

    return bucketId == o.bucketId && offset == o.offset;
  }

  @Override
  public int compareTo(final PIdentifiable o) {
    final PRID other = o.getIdentity();
    if (bucketId > other.bucketId)
      return 1;
    else if (bucketId < other.bucketId)
      return -1;

    if (offset > other.offset)
      return 1;
    else if (offset < other.offset)
      return -1;

    return 0;
  }
}
