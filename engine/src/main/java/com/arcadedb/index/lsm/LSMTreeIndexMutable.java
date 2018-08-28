/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * LSM-Tree index. The first page contains 2 bytes to store key and value types. The pages are populated from the head of the page
 * with the pointers to the pair key/value that starts from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (key/value pairs).
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 * <p>
 * HEADER ROOT PAGES (1st) = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),compactedPages(int:4),subIndexFileId(int:4),numberOfKeys(byte:1),keyType(byte:1)*]
 * <p>
 * HEADER Nst PAGE         = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4)]
 */
public class LSMTreeIndexMutable extends LSMTreeIndex {
  private int                   subIndexFileId = -1;
  private LSMTreeIndexCompacted subIndex       = null;

  private AtomicLong statsAdjacentSteps = new AtomicLong();

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize) throws IOException {
      return new LSMTreeIndexMutable(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      return new LSMTreeIndexMutable(database, name, true, filePath, id, mode, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      return new LSMTreeIndexMutable(database, name, false, filePath, id, mode, pageSize);
    }
  }

  /**
   * Called at creation time.
   */
  public LSMTreeIndexMutable(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    database.checkTransactionIsActive();
    createNewPage(0);
  }

  /**
   * Called at cloning time.
   */
  public LSMTreeIndexMutable(final Database database, final String name, final boolean unique, final String filePath, final byte[] keyTypes, final int pageSize,
      final COMPACTING_STATUS compactingStatus, final LSMTreeIndexCompacted subIndex) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    this.compactingStatus = compactingStatus;
    this.subIndex = subIndex;
    database.checkTransactionIsActive();
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndexMutable(final Database database, final String name, final boolean unique, final String filePath, final int id,
      final PaginatedFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, unique, filePath, id, mode, pageSize);

    final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);

    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    final int compactedPages = currentPage.readInt(pos);
    if (compactedPages > 0)
      compactingStatus = COMPACTING_STATUS.COMPACTED;
    else
      compactingStatus = COMPACTING_STATUS.NO;

    pos += INT_SERIALIZED_SIZE;

    subIndexFileId = currentPage.readInt(pos);

    pos += INT_SERIALIZED_SIZE;

    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
  }

  @Override
  public void close() {
    super.close();
    if (subIndex != null)
      subIndex.close();
  }

  @Override
  public void onAfterLoad() {
    if (subIndexFileId > -1) {
      try {
        subIndex = (LSMTreeIndexCompacted) database.getSchema().getFileById(subIndexFileId);
      } catch (Exception e) {
        LogManager.instance().info(this, "Invalid subindex for index '%s', ignoring it", name);
      }
    }
  }

  public LSMTreeIndexCompacted createNewForCompaction() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();

    return new LSMTreeIndexCompacted(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, pageSize);
  }

  public boolean compact() throws IOException, InterruptedException {
    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + name + "'");

    if (compactingStatus == COMPACTING_STATUS.IN_PROGRESS)
      throw new IllegalStateException("Index '" + name + "' is already compacting");

    compactingStatus = COMPACTING_STATUS.IN_PROGRESS;
    try {
      final LSMTreeIndexCompactor compactor = new LSMTreeIndexCompactor(this);
      return compactor.compact();
    } finally {
      compactingStatus = COMPACTING_STATUS.COMPACTED;
    }
  }

  @Override
  public void finalize() {
    close();
  }

  public LSMTreeIndexMutable copyPagesToNewFile(final int startingFromPage, final LSMTreeIndexCompacted subIndex) throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();

    final LSMTreeIndexMutable newIndex = new LSMTreeIndexMutable(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, pageSize,
        COMPACTING_STATUS.NO, subIndex);
    ((SchemaImpl) database.getSchema()).registerFile(newIndex);

    // KEEP METADATA AND LEAVE IT EMPTY
    newIndex.createNewPage(0);

    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        for (int i = 0; i < getTotalPages() - startingFromPage; ++i) {
          final BasePage currentPage = database.getTransaction().getPage(new PageId(newIndex.file.getFileId(), i + startingFromPage), pageSize);

          // COPY THE ENTIRE PAGE TO THE NEW INDEX
          final MutablePage newPage = newIndex.createNewPage(0);

          final ByteBuffer pageContent = currentPage.getContent();
          pageContent.rewind();
          newPage.getContent().put(pageContent);
        }

        // SWAP OLD WITH NEW INDEX
        ((SchemaImpl) database.getSchema()).swapIndexes(LSMTreeIndexMutable.this, newIndex);

        return null;
      }
    });

    return newIndex;
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) throws IOException {
    return new LSMTreeIndexCursor(this, ascendingOrder);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) throws IOException {
    if (ascendingOrder)
      return range(fromKeys, null);

    return range(null, fromKeys);
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys) throws IOException {
    return range(fromKeys, fromKeys);
  }

  @Override
  public IndexCursor range(final Object[] fromKeys, final Object[] toKeys) throws IOException {
    return new LSMTreeIndexCursor(this, true, fromKeys, toKeys);
  }

  public LSMTreeIndexPageIterator newPageIterator(final int pageId, final int currentEntryInPage, final boolean ascendingOrder) throws IOException {
    final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
    return new LSMTreeIndexPageIterator(this, page, currentEntryInPage, getHeaderSize(pageId), keyTypes, getCount(page), ascendingOrder);
  }

  public LSMTreeIndexCompacted getSubIndex() {
    return subIndex;
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    final Object[] convertedKeys = convertKeys(keys, keyTypes);

    final Set<RID> set = new HashSet<>();

    final Set<RID> removedRIDs = new HashSet<>();

    // NON COMPACTED INDEX, SEARCH IN ALL THE PAGES
    searchInNonCompactedIndex(convertedKeys, limit, set, removedRIDs);

    return set;
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    stats.put("adjacentSteps", statsAdjacentSteps.get());
    return stats;
  }

  public void removeTempSuffix() {
    super.removeTempSuffix();
    if (subIndex != null)
      subIndex.removeTempSuffix();
  }

  protected LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] convertedKeys, int mid, final int count,
      final int purpose) {

    int result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count);

    if (result > 0)
      return HIGHER;
    else if (result < 0)
      return LOWER;

    if (purpose == 0) {
      // EXISTS
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      return new LookupResult(true, false, mid, new int[] { currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)) + keySerializedSize });
    } else if (purpose == 1) {
      // RETRIEVE
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      // RETRIEVE ALL THE RESULTS
      final int firstKeyPos = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
      final int lastKeyPos = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);

      final int[] positionsArray = new int[lastKeyPos - firstKeyPos + 1];
      for (int i = firstKeyPos; i <= lastKeyPos; ++i)
        positionsArray[i - firstKeyPos] = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)) + keySerializedSize;

      return new LookupResult(true, false, lastKeyPos, positionsArray);
    }

    if (convertedKeys.length < keyTypes.length) {
      // PARTIAL MATCHING
      if (purpose == 2) {
        // ASCENDING ITERATOR: FIND THE MOST LEFT ITEM
        mid = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
      } else if (purpose == 3) {
        // DESCENDING ITERATOR
        mid = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);
      }
    }

    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
    return new LookupResult(true, false, mid, new int[] { currentPageBuffer.position() });
  }

  private int findLastEntryOfSameKey(final int count, final Binary currentPageBuffer, final Object[] keys, final int startIndexArray, int mid) {
    int result;// FIND THE MOST RIGHT ITEM
    for (int i = mid + 1; i < count; ++i) {
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)));

      result = 1;
      for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
        final byte keyType = keyTypes[keyIndex];
        if (keyType == BinaryTypes.TYPE_STRING) {
          // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
          result = comparator.compareStrings((byte[]) keys[keyIndex], currentPageBuffer);
        } else {
          final Object key = serializer.deserializeValue(database, currentPageBuffer, keyType);
          result = comparator.compare(keys[keyIndex], keyType, key, keyType);
        }

        if (result != 0)
          break;
      }

      if (result == 0) {
        mid = i;
        statsAdjacentSteps.incrementAndGet();
      } else
        break;
    }
    return mid;
  }

  private int findFirstEntryOfSameKey(final Binary currentPageBuffer, final Object[] keys, final int startIndexArray, int mid) {
    int result;
    for (int i = mid - 1; i >= 0; --i) {
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)));

      result = 1;
      for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
        if (keyTypes[keyIndex] == BinaryTypes.TYPE_STRING) {
          // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
          result = comparator.compareStrings((byte[]) keys[keyIndex], currentPageBuffer);
        } else {
          final Object key = serializer.deserializeValue(database, currentPageBuffer, keyTypes[keyIndex]);
          result = comparator.compare(keys[keyIndex], keyTypes[keyIndex], key, keyTypes[keyIndex]);
        }

        if (result != 0)
          break;
      }

      if (result == 0) {
        mid = i;
        statsAdjacentSteps.incrementAndGet();
      } else
        break;
    }
    return mid;
  }

  @Override
  protected MutablePage createNewPage(final int compactedPages) {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final MutablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);

    int pos = 0;
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    currentPage.writeInt(pos, 0); // ENTRIES COUNT
    pos += INT_SERIALIZED_SIZE;

    if (txPageCounter == 0) {
      currentPage.writeInt(pos, compactedPages); // COMPACTED PAGES
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeInt(pos, subIndex != null ? subIndex.getId() : -1); // SUB-INDEX FILE ID
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i)
        currentPage.writeByte(pos++, keyTypes[i]);
    }

    return currentPage;
  }

  private void searchInNonCompactedIndex(final Object[] convertedKeys, final int limit, final Set<RID> set, final Set<RID> removedRIDs) {
    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        // SEARCH FROM THE LAST PAGE BACK
        final int totalPages = getTotalPages();

        for (int p = totalPages - 1; p > -1; --p) {
          final BasePage currentPage = database.getTransaction().getPage(new PageId(file.getFileId(), p), pageSize);
          final Binary currentPageBuffer = new Binary(currentPage.slice());
          final int count = getCount(currentPage);

          if (count < 1)
            continue;

          if (!lookupInPageAndAddInResultset(currentPage, currentPageBuffer, count, convertedKeys, limit, set, removedRIDs))
            return null;
        }

        if (subIndex != null)
          // CONTINUE ON THE SUB-INDEX
          subIndex.searchInCompactedIndex(convertedKeys, limit, set, removedRIDs);

        return null;
      }
    });
  }
}
