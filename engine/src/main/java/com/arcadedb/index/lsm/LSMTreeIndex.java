/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LockContext;
import com.arcadedb.utility.LogManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * Abstract class for LSM-based indexes.
 */
public abstract class LSMTreeIndex extends PaginatedComponent implements Index {
  public static final    int    DEF_PAGE_SIZE       = 2 * 1024 * 1024;
  public static final    RID    REMOVED_ENTRY_RID   = new RID(null, -1, -1l);
  public static final    String UNIQUE_INDEX_EXT    = "utidx";
  public static final    String NOTUNIQUE_INDEX_EXT = "nutidx";
  protected static final String TEMP_EXT            = "temp_";

  protected static final LSMTreeIndexCompacted.LookupResult LOWER  = new LSMTreeIndexCompacted.LookupResult(false, true, 0, null);
  protected static final LSMTreeIndexCompacted.LookupResult HIGHER = new LSMTreeIndexCompacted.LookupResult(false, true, 0, null);

  protected final    BinaryComparator  comparator;
  protected final    BinarySerializer  serializer;
  protected final    byte              valueType         = BinaryTypes.TYPE_COMPRESSED_RID;
  protected final    boolean           unique;
  protected          byte[]            keyTypes;
  protected          LockContext       lock              = new LockContext();
  protected volatile COMPACTING_STATUS compactingStatus  = COMPACTING_STATUS.NO;
  protected volatile boolean           dropWhenCollected = false;

  public enum COMPACTING_STATUS {NO, IN_PROGRESS, COMPACTED}

  protected static class LookupResult {
    public final boolean found;
    public final boolean outside;
    public final int     keyIndex;
    public final int[]   valueBeginPositions;

    public LookupResult(final boolean found, final boolean outside, final int keyIndex, final int[] valueBeginPositions) {
      this.found = found;
      this.outside = outside;
      this.keyIndex = keyIndex;
      this.valueBeginPositions = valueBeginPositions;
    }
  }

  /**
   * Called at creation time.
   */
  protected LSMTreeIndex(final Database database, final String name, final boolean unique, String filePath, final String ext, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), ext, mode, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
  }

  /**
   * Called at cloning time.
   */
  protected LSMTreeIndex(final Database database, final String name, final boolean unique, String filePath, final String ext, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), TEMP_EXT + ext, PaginatedFile.MODE.READ_WRITE, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
  }

  /**
   * Called at load time (1st page only).
   */
  protected LSMTreeIndex(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
  }

  protected abstract MutablePage createNewPage(final int compactedPages);

  protected abstract LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] convertedKeys, int mid, final int count,
      final int purpose);

  @Override
  public void finalize() {
    close();
  }

  public void lazyDrop() {
    dropWhenCollected = true;
  }

  @Override
  public void close() {
    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (dropWhenCollected) {
          try {
            LogManager.instance().debug(this, "Finalizing deletion of index '%s'...", name);
            if (database.isOpen())
              ((SchemaImpl) database.getSchema()).removeIndex(getName());
            drop();
          } catch (IOException e) {
            LogManager.instance().error(this, "Error on dropping the index '%s'", e, name);
          }
        }
        return null;
      }
    });
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

  public void removeTempSuffix() {
    final String fileName = file.getFileName();

    final int extPos = fileName.lastIndexOf('.');
    if (fileName.substring(extPos + 1).startsWith(TEMP_EXT)) {
      try {
        file.rename(fileName.substring(0, extPos) + "." + fileName.substring(extPos + TEMP_EXT.length() + 1));
      } catch (FileNotFoundException e) {
        throw new IndexException("Cannot rename temp file", e);
      }
    }
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    return stats;
  }

  /**
   * Lookups for an entry in the index by using dichotomy search.
   *
   * @param purpose 0 = exists, 1 = retrieve, 2 = ascending iterator, 3 = descending iterator
   *
   * @return always an LookupResult object, never null
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final Binary currentPageBuffer, final Object[] convertedKeys, final int purpose) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (convertedKeys.length > keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + convertedKeys.length + " items, while the index defined " + keyTypes.length + " items");

    if ((purpose == 0 || purpose == 1) && convertedKeys.length != keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + convertedKeys.length + " items, while the index defined " + keyTypes.length + " items");

    if (count == 0)
      // EMPTY, NOT FOUND
      return new LookupResult(false, true, 0, null);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize(pageNum);

    LookupResult result;

    // CHECK THE BOUNDARIES FIRST (LOWER THAN THE FIRST)
    result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, low, count, purpose);
    if (result == LOWER)
      return new LookupResult(false, true, low, null);
    else if (result != HIGHER)
      return result;

    // CHECK THE BOUNDARIES FIRST (HIGHER THAN THE LAST)
    result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, high, count, purpose);
    if (result == HIGHER)
      return new LookupResult(false, true, count, null);
    else if (result != LOWER)
      return result;

    while (low <= high) {
      int mid = (low + high) / 2;

      result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count, purpose);

      if (result == HIGHER)
        low = mid + 1;
      else if (result == LOWER)
        high = mid - 1;
      else
        return result;
    }

    return new LookupResult(false, false, low, null);
  }

  protected void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (keys == null)
      throw new IllegalArgumentException("Keys parameter is null");

    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + name + "'");

    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot put an entry in the index with a partial key");

    if (unique && checkForUnique) {
      final Set<RID> result = get(keys, 1);
      if (!result.isEmpty())
        throw new DuplicatedKeyException(name, Arrays.toString(keys));
    }

    checkForNulls(keys);

    database.checkTransactionIsActive();

    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() {
        final int txPageCounter = getTotalPages();

        if (txPageCounter < 1)
          throw new IllegalArgumentException("Cannot update the index '" + name + "' because the file is invalid");

        int pageNum = txPageCounter - 1;

        try {
          MutablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

          TrackableBinary currentPageBuffer = currentPage.getTrackable();

          int count = getCount(currentPage);

          final Object[] convertedKeys = convertKeys(keys, keyTypes);

          final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, convertedKeys, unique ? 0 : 1);

          // WRITE KEY/VALUE PAIRS FIRST
          final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
          writeEntry(keyValueContent, convertedKeys, rid);

          int keyValueFreePosition = getValuesFreePosition(currentPage);

          int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;
          boolean newPage = false;
          if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
            // NO SPACE LEFT, CREATE A NEW PAGE
            newPage = true;

            currentPage = createNewPage(0);
            currentPageBuffer = currentPage.getTrackable();
            pageNum = currentPage.getPageId().getPageNumber();
            count = 0;
            keyIndex = 0;
            keyValueFreePosition = currentPage.getMaxContentSize();
          }

          keyValueFreePosition -= keyValueContent.size();

          // WRITE KEY/VALUE PAIR CONTENT
          currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

          final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
          if (keyIndex < count)
            // NOT LAST KEY, SHIFT POINTERS TO THE RIGHT
            currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

          currentPageBuffer.putInt(startPos, keyValueFreePosition);

          setCount(currentPage, count + 1);
          setValuesFreePosition(currentPage, keyValueFreePosition);

          LogManager.instance()
              .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
                  count + 1, newPage);

        } catch (IOException e) {
          throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
        }
        return null;
      }
    });
  }

  protected void internalRemove(final Object[] keys, final RID rid) {
    if (keys == null)
      throw new IllegalArgumentException("Keys parameter is null");

    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + name + "'");

    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");

    checkForNulls(keys);

    database.checkTransactionIsActive();

    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() {
        final int txPageCounter = getTotalPages();

        if (txPageCounter < 1)
          throw new IllegalArgumentException("Cannot update the index '" + name + "' because the file is invalid");

        int pageNum = txPageCounter - 1;

        try {
          MutablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

          TrackableBinary currentPageBuffer = currentPage.getTrackable();

          int count = getCount(currentPage);

          final RID removedRID = rid != null ? getRemovedRID(rid) : REMOVED_ENTRY_RID;

          final Object[] convertedKeys = convertKeys(keys, keyTypes);

          final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, convertedKeys, 1);
          if (result.found) {
            boolean exit = false;

            for (int i = result.valueBeginPositions.length - 1; !exit && i > -1; --i) {
              currentPageBuffer.position(result.valueBeginPositions[i]);

              final Object[] values = readEntryValues(currentPageBuffer);

              for (int v = values.length - 1; v > -1; --v) {
                final RID currentRID = (RID) values[v];

                if (rid != null) {
                  if (rid.equals(currentRID)) {
                    // FOUND
                    exit = true;
                    break;
                  } else if (removedRID.equals(currentRID))
                    // ALREADY DELETED
                    return null;
                }

                if (currentRID.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && currentRID.getPosition() == REMOVED_ENTRY_RID.getPosition()) {
                  // ALREADY DELETED
                  return null;
                }
              }
            }
          }

          // WRITE KEY/VALUE PAIRS FIRST
          final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
          writeEntry(keyValueContent, keys, removedRID);

          int keyValueFreePosition = getValuesFreePosition(currentPage);

          int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;
          boolean newPage = false;
          if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
            // NO SPACE LEFT, CREATE A NEW PAGE
            newPage = true;

            currentPage = createNewPage(0);
            currentPageBuffer = currentPage.getTrackable();
            pageNum = currentPage.getPageId().getPageNumber();
            count = 0;
            keyIndex = 0;
            keyValueFreePosition = currentPage.getMaxContentSize();
          }

          keyValueFreePosition -= keyValueContent.size();

          // WRITE KEY/VALUE PAIR CONTENT
          currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

          final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
          if (keyIndex < count)
            // NOT LAST KEY, SHIFT POINTERS TO THE RIGHT
            currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

          currentPageBuffer.putInt(startPos, keyValueFreePosition);

          setCount(currentPage, count + 1);
          setValuesFreePosition(currentPage, keyValueFreePosition);

          LogManager.instance()
              .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
                  count + 1, newPage);

        } catch (IOException e) {
          throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
        }

        return null;
      }
    });
  }

  protected void writeEntry(final Binary buffer, final Object[] keys, final Object rid) {
    buffer.clear();

    // WRITE KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      serializer.serializeValue(buffer, keyTypes[i], keys[i]);

    writeEntryValue(buffer, rid);
  }

  protected void writeEntry(final Binary buffer, final Object[] keys, final Object[] rids) {
    buffer.clear();

    // WRITE KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      serializer.serializeValue(buffer, keyTypes[i], keys[i]);

    writeEntryValues(buffer, rids);
  }

  /**
   * Reads the keys and returns the serialized size.
   */
  protected int getSerializedKeySize(final Binary buffer, final int keyLength) {
    final int startsAt = buffer.position();
    for (int keyIndex = 0; keyIndex < keyLength; ++keyIndex)
      serializer.deserializeValue(database, buffer, keyTypes[keyIndex]);

    return buffer.position() - startsAt;
  }

  protected Object[] convertKeys(final Object[] keys, final byte[] keyTypes) {
    if (keys != null) {
      final Object[] convertedKeys = new Object[keys.length];
      for (int i = 0; i < keys.length; ++i) {
        convertedKeys[i] = Type.convert(database, keys[i], BinaryTypes.getClassFromType(keyTypes[i]));

        if (convertedKeys[i] instanceof String)
          // OPTIMIZATION: ALWAYS CONVERT STRINGS TO BYTE[]
          convertedKeys[i] = ((String) convertedKeys[i]).getBytes();
      }
      return convertedKeys;
    }
    return keys;
  }

  protected int compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object keys[], final int mid, final int count) {
    final int contentPos = currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE));
    if (contentPos < startIndexArray + (count * INT_SERIALIZED_SIZE))
      throw new IndexException("Internal error: invalid content position " + contentPos + " is < of " + (startIndexArray + (count * INT_SERIALIZED_SIZE)));

    currentPageBuffer.position(contentPos);

    int result = -1;
    for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
      // GET THE KEY
      final Object key = keys[keyIndex];

      if (keyTypes[keyIndex] == BinaryTypes.TYPE_STRING) {
        // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
        result = comparator.compareStrings((byte[]) key, currentPageBuffer);
      } else {
        final Object keyValue = serializer.deserializeValue(database, currentPageBuffer, keyTypes[keyIndex]);
        result = comparator.compare(key, keyTypes[keyIndex], keyValue, keyTypes[keyIndex]);
      }

      if (result != 0)
        break;
    }

    return result;
  }

  protected int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    if (pageNum == 0)
      size += INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + keyTypes.length;

    return size;
  }

  protected static int compareKeys(final BinaryComparator comparator, final byte[] keyTypes, final Object[] keys1, final Object[] keys2) {
    final int minKeySize = Math.min(keys1.length, keys2.length);

    for (int k = 0; k < minKeySize; ++k) {
      final int result = comparator.compare(keys1[k], keyTypes[k], keys2[k], keyTypes[k]);
      if (result < 0)
        return -1;
      else if (result > 0)
        return 1;
    }
    return 0;
  }

  private void writeEntryValues(final Binary buffer, final Object[] values) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(buffer, BinaryTypes.TYPE_INT, values.length);

    // WRITE VALUES
    for (int i = 0; i < values.length; ++i)
      serializer.serializeValue(buffer, valueType, values[i]);
  }

  private void writeEntryValue(final Binary buffer, final Object value) {
    // WRITE NUMBER OF VALUES
    serializer.serializeValue(buffer, BinaryTypes.TYPE_INT, 1);

    // WRITE VALUES
    serializer.serializeValue(buffer, valueType, value);
  }

  protected RID[] readEntryValues(final Binary buffer) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final RID[] rids = new RID[items];

    for (int i = 0; i < rids.length; ++i)
      rids[i] = (RID) serializer.deserializeValue(database, buffer, valueType);

    return rids;
  }

  private void readEntryValues(final Binary buffer, final List<RID> list) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final Object[] rids = new Object[items];

    for (int i = 0; i < rids.length; ++i)
      list.add((RID) serializer.deserializeValue(database, buffer, valueType));
  }

  private List<RID> readAllValuesFromResult(final Binary currentPageBuffer, final LookupResult result) {
    final List<RID> allValues = new ArrayList<>();
    for (int i = 0; i < result.valueBeginPositions.length; ++i) {
      currentPageBuffer.position(result.valueBeginPositions[i]);
      readEntryValues(currentPageBuffer, allValues);
    }
    return allValues;
  }

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  protected void setCount(final MutablePage currentPage, final int newCount) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newCount);
  }

  protected Object[] checkForNulls(final Object keys[]) {
    if (keys != null)
      for (int i = 0; i < keys.length; ++i)
        if (keys[i] == null)
          throw new IllegalArgumentException("Indexed key cannot be NULL");
    return keys;
  }

  protected boolean lookupInPageAndAddInResultset(final BasePage currentPage, final Binary currentPageBuffer, final int count, final Object[] convertedKeys,
      int limit, final Set<RID> set, final Set<RID> removedRIDs) {
    final LookupResult result = lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, convertedKeys, 1);
    if (result.found) {
      // REAL ALL THE ENTRIES
      final List<RID> allValues = readAllValuesFromResult(currentPageBuffer, result);

      // START FROM THE LAST ENTRY
      for (int i = allValues.size() - 1; i > -1; --i) {
        final RID rid = allValues.get(i);

        if (rid.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && rid.getPosition() == REMOVED_ENTRY_RID.getPosition()) {
          if (set.contains(rid))
            continue;
          else {
            // DELETED ITEM
            set.clear();
            return false;
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
          return false;
        }
      }
    }
    return true;
  }

  protected int getValuesFreePosition(final BasePage currentPage) {
    return currentPage.readInt(0);
  }

  protected void setValuesFreePosition(final MutablePage currentPage, final int newValuesFreePosition) {
    currentPage.writeInt(0, newValuesFreePosition);
  }

  protected RID getRemovedRID(final RID rid) {
    return new RID(database, (rid.getBucketId() + 2) * -1, rid.getPosition());
  }

  protected RID getOriginalRID(final RID rid) {
    return new RID(database, (rid.getBucketId() * -1) - 2, rid.getPosition());
  }
}
