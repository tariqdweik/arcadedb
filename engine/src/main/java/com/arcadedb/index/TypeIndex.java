/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.schema.SchemaImpl;

import java.io.IOException;
import java.util.*;

/**
 * It's backed by one or multiple bucket sub-indexes.
 */
public class TypeIndex implements RangeIndex {
  private List<Index> indexes = new ArrayList<>();

  /**
   * Returns the involved sub-indexes for a lookup by key.
   */
  protected List<? extends Index> getSubIndexesForLookup(final Object[] keys, boolean inclusive) {
    return indexes;
  }

  /**
   * Returns the involved sub-indexes for a lookup by key.
   */
  protected List<RangeIndex> getSubIndexesForRange(final Object[] keys, boolean inclusive) {
    return null;//(List<RangeIndex>) indexes;
  }

  /**
   * Returns the involved sub-index to insert a new entry.
   */
  protected Index getSubIndexForPut(final Object[] keys) {
    return null;
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys, final boolean inclusive) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(getSubIndexesForRange(null, inclusive), true, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(getSubIndexesForRange(null, true), ascendingOrder, -1);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    return new MultiIndexCursor(getSubIndexesForRange(fromKeys, inclusive), ascendingOrder, -1);
  }

  @Override
  public IndexCursor range(final Object[] beginKeys, final boolean beginKeysInclusive, final Object[] endKeys, boolean endKeysInclusive) {
    if (!supportsOrderedIterations())
      throw new UnsupportedOperationException("Index '" + getName() + "' does not support ordered iterations");

    final List<IndexCursor> cursors = new ArrayList<>(indexes.size());
    for (Index index : indexes)
      cursors.add(((RangeIndex) index).range(beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));

    return new MultiIndexCursor(cursors, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    final Set<Identifiable> result = new HashSet<>();
    for (Index index : indexes) {
      final IndexCursor cursor = index.get(keys);
      while (cursor.hasNext())
        result.add(cursor.next());
    }
    return new IndexCursorCollection(result);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    final Set<Identifiable> result = new HashSet<>();
    for (Index index : indexes) {
      final IndexCursor cursor = index.get(keys, limit > -1 ? result.size() - limit : -1);
      while (cursor.hasNext())
        result.add(cursor.next());
    }
    return new IndexCursorCollection(result);
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
  public void remove(final Object[] keys, final Identifiable rid) {
    getSubIndexForPut(keys).remove(keys, rid);
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    boolean result = false;
    for (Index index : indexes)
      if (index.compact())
        result = true;
    return result;
  }

  @Override
  public boolean isCompacting() {
    for (Index index : indexes)
      if (index.isCompacting())
        return true;
    return false;
  }

  @Override
  public boolean scheduleCompaction() {
    boolean result = false;
    for (Index index : indexes)
      if (index.scheduleCompaction())
        result = true;
    return result;
  }

  @Override
  public SchemaImpl.INDEX_TYPE getType() {
    return indexes.get(0).getType();
  }

  @Override
  public String getTypeName() {
    return indexes.get(0).getTypeName();
  }

  @Override
  public String[] getPropertyNames() {
    return indexes.get(0).getPropertyNames();
  }

  @Override
  public void close() {
    for (Index index : indexes)
      index.close();
  }

  @Override
  public void drop() {
    for (Index index : indexes)
      index.drop();
  }

  @Override
  public String getName() {
    return indexes.get(0).getName();
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    for (Index index : indexes)
      stats.putAll(index.getStats());
    return stats;
  }

  @Override
  public boolean isUnique() {
    return indexes.get(0).isUnique();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return indexes.get(0).supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public long build(final BuildIndexCallback callback) {
    long total = 0;
    for (Index index : indexes)
      total += index.build(callback);
    return total;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof TypeIndex))
      return false;

    final TypeIndex index2 = (TypeIndex) obj;

    if (!getName().equals(index2.getName()))
      return false;

    if (getAssociatedBucketId() != index2.getAssociatedBucketId())
      return false;

    final String[] index1Properties = getPropertyNames();
    final String[] index2Properties = index2.getPropertyNames();

    if (index1Properties.length != index2Properties.length)
      return false;

    for (int p = 0; p < index1Properties.length; ++p) {
      if (!index1Properties[p].equals(index2Properties[p]))
        return false;
    }

    return true;
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

  public void addSubIndex(final Index index) {
    if (index instanceof TypeIndex)
      throw new IllegalArgumentException("Invalid subIndex " + index);

    indexes.add(index);
  }
}
