/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.schema.SchemaImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * It's backed by one or multiple bucket sub-indexes.
 */
public class TypeIndex implements RangeIndex {
  private RangeIndex[] indexes;

  public TypeIndex(final RangeIndex[] indexes) {
    this.indexes = indexes;
  }

  /**
   * Returns the involved sub-indexes for a lookup by key.
   */
  protected List<RangeIndex> getSubIndexesForLookup(final Object[] keys, boolean inclusive) {
    return null;
  }

  /**
   * Returns the involved sub-index to insert a new entry.
   */
  protected RangeIndex getSubIndexForPut(final Object[] keys) {
    return null;
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys, final boolean inclusive) {
    return new MultiIndexCursor(getSubIndexesForLookup(null, inclusive), true, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    return new MultiIndexCursor(getSubIndexesForLookup(null, true), ascendingOrder, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    return new MultiIndexCursor(getSubIndexesForLookup(fromKeys, inclusive), ascendingOrder, -1);
  }

  @Override
  public IndexCursor range(final Object[] beginKeys, final boolean beginKeysInclusive, final Object[] endKeys, boolean endKeysInclusive) {
    final List<IndexCursor> cursors = new ArrayList<>(indexes.length);
    for (RangeIndex index : indexes)
      cursors.add(index.range(beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));

    return new MultiIndexCursor(cursors, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return new MultiIndexCursor(getSubIndexesForLookup(keys, true), true, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    return new MultiIndexCursor(getSubIndexesForLookup(keys, true), true, limit);
  }

  @Override
  public void put(final Object[] keys, final RID[] rid) {
    getSubIndexForPut(keys).put(keys, rid);
  }

  @Override
  public void remove(final Object[] keys) {
    getSubIndexForPut(keys).remove(keys);
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    getSubIndexForPut(keys).remove(keys, rid);
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    boolean result = false;
    for (RangeIndex index : indexes)
      if (index.compact())
        result = true;
    return result;
  }

  @Override
  public boolean isCompacting() {
    for (RangeIndex index : indexes)
      if (index.isCompacting())
        return true;
    return false;
  }

  @Override
  public boolean scheduleCompaction() {
    boolean result = false;
    for (RangeIndex index : indexes)
      if (index.scheduleCompaction())
        result = true;
    return result;
  }

  @Override
  public SchemaImpl.INDEX_TYPE getType() {
    return indexes[0].getType();
  }

  @Override
  public String getTypeName() {
    return indexes[0].getTypeName();
  }

  @Override
  public String[] getPropertyNames() {
    return indexes[0].getPropertyNames();
  }

  @Override
  public void close() {
    for (RangeIndex index : indexes)
      index.close();
  }

  @Override
  public void drop() {
    for (RangeIndex index : indexes)
      index.drop();
  }

  @Override
  public String getName() {
    return indexes[0].getName();
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    for (RangeIndex index : indexes)
      stats.putAll(index.getStats());
    return stats;
  }

  @Override
  public boolean isUnique() {
    return indexes[0].isUnique();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return indexes[0].supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public long build(final BuildIndexCallback callback) {
    long total = 0;
    for (RangeIndex index : indexes)
      total += index.build(callback);
    return total;
  }

  @Override
  public void setMetadata(final String name, final String[] propertyNames, final int associatedBucketId) {
    throw new UnsupportedOperationException("setMetadata");
  }

  @Override
  public int getFileId() {
    throw new UnsupportedOperationException("getFileId");
  }

  @Override
  public PaginatedComponent getPaginatedComponent() {
    throw new UnsupportedOperationException("getPaginatedComponent");
  }

  @Override
  public int getAssociatedBucketId() {
    throw new UnsupportedOperationException("getAssociatedBucketId");
  }
}
