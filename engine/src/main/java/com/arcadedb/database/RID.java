/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import java.io.Serializable;

/**
 * Immutable class.
 */
public class RID implements Identifiable, Comparable<Identifiable>, Serializable {
  private final Database database;
  private       int      bucketId;
  private       long     offset;

  public RID(final Database database, final int bucketId, final long offset) {
    this.database = database;
    this.bucketId = bucketId;
    this.offset = offset;
  }

  public RID(final Database database, String value) {
    this.database = database;
    if (!value.startsWith("#"))
      throw new IllegalArgumentException("RID as string is not valid");

    value = value.substring(1);

    final String[] parts = value.split(":", 2);
    this.bucketId = Integer.parseInt(parts[0]);
    this.offset = Long.parseLong(parts[1]);
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
  public RID getIdentity() {
    return this;
  }

  @Override
  public Record getRecord() {
    return database.lookupByRID(this, false);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof Identifiable))
      return false;

    final RID o = ((Identifiable) obj).getIdentity();

    return bucketId == o.bucketId && offset == o.offset;
  }

  @Override
  public int hashCode() {
    int result = bucketId;
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public int compareTo(final Identifiable o) {
    final RID other = o.getIdentity();
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
