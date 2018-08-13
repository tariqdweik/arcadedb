/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.io.IOException;
import java.util.*;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * Abstract class for LSM-based indexes.
 */
public abstract class IndexLSMAbstract extends PaginatedComponent implements Index {
  public static final int DEF_PAGE_SIZE     = 4 * 1024 * 1024;
  public static final RID REMOVED_ENTRY_RID = new RID(null, -1, -1l);

  protected final    BinarySerializer serializer;
  protected          byte[]           keyTypes;
  protected          byte             valueType;
  protected volatile boolean          compacting = false;
  protected final    boolean          unique;

  protected abstract void internalPut(Object[] keys, RID rid, boolean checkForUnique);

  protected abstract void internalRemove(Object[] keys, RID rid);

  protected static class LookupResult {
    public final boolean found;
    public final int     keyIndex;
    public final int[]   valueBeginPositions;

    public LookupResult(final boolean found, final int keyIndex, final int[] valueBeginPositions) {
      this.found = found;
      this.keyIndex = keyIndex;
      this.valueBeginPositions = valueBeginPositions;
    }
  }

  /**
   * Called at creation time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), ext, mode, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = BinaryTypes.TYPE_COMPRESSED_RID;
  }

  /**
   * Called at cloning time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), "temp_" + ext, PaginatedFile.MODE.READ_WRITE, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = BinaryTypes.TYPE_COMPRESSED_RID;
  }

  /**
   * Called at load time (1st page only).
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
  }

  /**
   * @param purpose 0 = exists, 1 = retrieve
   *
   * @return
   */
  protected abstract LookupResult searchInPage(final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys, final int count,
      final int purpose);

  public void removeTempSuffix() {
    // TODO
  }

  @Override
  public Set<RID> get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public void put(final Object[] keys, final RID rid) {
    put(keys, rid, true);
  }

  @Override
  public void put(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (rid == null)
      throw new IllegalArgumentException("RID is null");

    internalPut(keys, rid, checkForUnique);
  }

  @Override
  public void remove(final Object[] keys) {
    internalRemove(keys, null);
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    internalRemove(keys, rid);
  }

  public boolean isUnique() {
    return unique;
  }

  @Override
  public int getFileId() {
    return file.getFileId();
  }

  @Override
  public String toString() {
    return name;
  }

  public byte[] getKeyTypes() {
    return keyTypes;
  }

  public boolean isDeletedEntry(final Object rid) {
    return ((RID) rid).getBucketId() < 0;
  }

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(0);
  }

  protected Object[] checkForNulls(final Object keys[]) {
    if (keys != null)
      for (int i = 0; i < keys.length; ++i)
        if (keys[i] == null)
          throw new IllegalArgumentException("Indexed key cannot be NULL");
    return keys;
  }

  protected int getEntriesFreePosition(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  protected void setEntriesFreePosition(final ModifiablePage currentPage, final int newKeyValueFreePosition) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newKeyValueFreePosition);
  }

  protected RID getRemovedRID(final RID rid) {
    return new RID(database, (rid.getBucketId() + 2) * -1, rid.getPosition());
  }

  protected RID getOriginalRID(final RID rid) {
    return new RID(database, (rid.getBucketId() * -1) - 2, rid.getPosition());
  }

  protected void writeEntryValues(final Binary buffer, final Object[] values) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(buffer, BinaryTypes.TYPE_INT, values.length);

    // WRITE VALUES
    for (int i = 0; i < values.length; ++i)
      serializer.serializeValue(buffer, valueType, values[i]);
  }

  protected void writeEntryValue(final Binary buffer, final Object value) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(buffer, BinaryTypes.TYPE_INT, 1);

    // WRITE VALUES
    serializer.serializeValue(buffer, valueType, value);
  }

  protected void updateEntryValue(final Binary buffer, final int valueIndex, final Object value) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    if (valueIndex > items - 1)
      throw new IllegalArgumentException("Cannot update value index " + valueIndex + " in value container with only " + items + " items");

    // MOVE TO THE LAST ITEM
    buffer.position(buffer.position() + (BinaryTypes.getTypeSize(valueType) * valueIndex));

    // WRITE VALUES
    serializer.serializeValue(buffer, valueType, value);
  }

  protected Object[] readEntryValues(final Binary buffer) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final Object[] rids = new Object[items];

    for (int i = 0; i < rids.length; ++i)
      rids[i] = serializer.deserializeValue(database, buffer, valueType);

    return rids;
  }

  protected void readEntryValues(final Binary buffer, final List list) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final Object[] rids = new Object[items];

    for (int i = 0; i < rids.length; ++i)
      list.add(serializer.deserializeValue(database, buffer, valueType));
  }

  protected List<Object> readAllValues(final Binary currentPageBuffer, final LookupResult result) {
    final List<Object> allValues = new ArrayList<>();
    for (int i = 0; i < result.valueBeginPositions.length; ++i) {
      currentPageBuffer.position(result.valueBeginPositions[i]);
      readEntryValues(currentPageBuffer, allValues);
    }
    return allValues;
  }

  protected void checkUniqueConstraint(final Object[] keys, final TrackableBinary currentPageBuffer, final LookupResult result) {
    // CHECK FOR DUPLICATES
    final List<Object> allValues = readAllValues(currentPageBuffer, result);

    final Set<RID> removedRIDs = new HashSet<>();

    for (int i = allValues.size() - 1; i > -1; --i) {
      final RID valueAsRid = (RID) allValues.get(i);
      if (valueAsRid.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && valueAsRid.getPosition() == REMOVED_ENTRY_RID.getPosition())
        // DELETED ITEM, FINE
        break;

      if (valueAsRid.getBucketId() < 0) {
        // RID DELETED, SKIP THE RID
        removedRIDs.add(getOriginalRID(valueAsRid));
        continue;
      }

      if (removedRIDs.contains(valueAsRid))
        // ALREADY FOUND AS DELETED, FINE
        continue;

      throw new DuplicatedKeyException(name, Arrays.toString(keys));
    }
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    try {
      final Set<RID> set = new HashSet<>();

      final int totalPages = getTotalPages();

      final Set<RID> removedRIDs = new HashSet<>();

      // SEARCH FROM THE LAST PAGE BACK
      for (int p = totalPages - 1; p > -1; --p) {
        final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), p), pageSize);
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = getCount(currentPage);

        final LookupResult result = searchInPage(currentPage, currentPageBuffer, keys, count, 1);
        if (result != null && result.found) {
          // REAL ALL THE ENTRIES
          final List<Object> allValues = readAllValues(currentPageBuffer, result);

          // START FROM THE LAST ENTRY
          for (int i = allValues.size() - 1; i > -1; --i) {
            RID rid = (RID) allValues.get(i);

            if (rid.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && rid.getPosition() == REMOVED_ENTRY_RID.getPosition()) {
              if (set.contains(rid))
                continue;
              else {
                // DELETED ITEM
                set.clear();
                return set;
              }
            }

            if (rid.getBucketId() < 0) {
              // RID DELETED, SKIP THE RID
              final RID originalRID = getOriginalRID(rid);
              if (!set.contains(originalRID))
                removedRIDs.add(originalRID);
              continue;
            }

            if (removedRIDs.contains(rid))
              // ALREADY FOUND AS DELETED
              continue;

            set.add(rid);

            if (limit > -1 && set.size() >= limit) {
              return set;
            }
          }
        }
      }

      return set;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }
}
