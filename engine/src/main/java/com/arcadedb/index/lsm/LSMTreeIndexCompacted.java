/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexCursor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
public class LSMTreeIndexCompacted extends LSMTreeIndex {
  /**
   * Called at creation time.
   */
  public LSMTreeIndexCompacted(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    database.checkTransactionIsActive();
    createNewPage(0);
  }

  /**
   * Called at cloning time.
   */
  public LSMTreeIndexCompacted(final Database database, final String name, final boolean unique, final String filePath, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    this.compactingStatus = COMPACTING_STATUS.COMPACTED;
    database.checkTransactionIsActive();
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndexCompacted(final Database database, final String name, final boolean unique, final String filePath, final int id,
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
    pos += INT_SERIALIZED_SIZE; // SKIP SUBINDEX IT

    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) throws IOException {
    throw new UnsupportedOperationException("range");
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) throws IOException {
    throw new UnsupportedOperationException("range");
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys) throws IOException {
    throw new UnsupportedOperationException("range");
  }

  @Override
  public IndexCursor range(final Object[] fromKeys, final Object[] toKeys) throws IOException {
    throw new UnsupportedOperationException("range");
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    final Object[] convertedKeys = convertKeys(keys, keyTypes);

    try {
      final Set<RID> set = new HashSet<>();

      final Set<RID> removedRIDs = new HashSet<>();

      // SEARCH IN COMPACTED INDEX
      searchInCompactedIndex(convertedKeys, limit, set, removedRIDs);

      return set;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  public MutablePage appendDuringCompaction(final Binary keyValueContent, MutablePage currentPage, TrackableBinary currentPageBuffer, final int pagesToCompact,
      final Object[] keys, final RID[] rids) {
    if( keys == null )
      throw new IllegalArgumentException("Keys parameter is null");

    if (currentPage == null) {
      // CREATE A NEW PAGE
      final int txPageCounter = getTotalPages();
      try {
        currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), txPageCounter), pageSize, true);
        currentPageBuffer = currentPage.getTrackable();
      } catch (IOException e) {
        throw new DatabaseOperationException(
            "Cannot append key '" + Arrays.toString(keys) + "' with value " + Arrays.toString(rids) + " in index '" + name + "'", e);
      }
    }

    int count = getCount(currentPage);

    int pageNum = currentPage.getPageId().getPageNumber();

    final Object[] convertedKeys = convertKeys(keys, keyTypes);

    writeEntry(keyValueContent, convertedKeys, rids);

    int keyValueFreePosition = getValuesFreePosition(currentPage);

    if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
      // NO SPACE LEFT, CREATE A NEW PAGE
      currentPage = createNewPage(pagesToCompact);
      currentPageBuffer = currentPage.getTrackable();
      pageNum = currentPage.getPageId().getPageNumber();
      count = 0;
      keyValueFreePosition = currentPage.getMaxContentSize();
    }

    keyValueFreePosition -= keyValueContent.size();

    // WRITE KEY/VALUE PAIR CONTENT
    currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

    final int startPos = getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE);
    currentPageBuffer.putInt(startPos, keyValueFreePosition);

    // TODO: !!!USE THE BF ON THE 1ST PAGE ONLY!!!
    // ADD THE ITEM IN THE BF
//    final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
//        getBFSeed(currentPage));
//
//    bf.add(BinaryTypes.getHash32(convertedKeys, bfKeyDepth));

    setCount(currentPage, count + 1);
    setValuesFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
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

      return new LookupResult(true, false, mid, new int[] { currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)) + keySerializedSize });
    }

    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
    return new LookupResult(true, false, mid, new int[] { currentPageBuffer.position() });
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

      currentPage.writeInt(pos, -1); // SUB-INDEX FILE ID
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i)
        currentPage.writeByte(pos++, keyTypes[i]);
    }

    return currentPage;
  }

  protected void searchInCompactedIndex(final Object[] convertedKeys, final int limit, final Set<RID> set, final Set<RID> removedRIDs) throws IOException {
    // JUMP ROOT PAGES BEFORE LOADING THE PAGE WITH THE KEY/VALUES
    final int totalPages = getTotalPages();

    for (int i = 0; i < totalPages; ) {
      final BasePage rootPage = database.getTransaction().getPage(new PageId(file.getFileId(), i), pageSize);

      final int rootPageCount = getCount(rootPage);

      if (rootPageCount < 1)
        break;

      final Binary rootPageBuffer = new Binary(rootPage.slice());
      final LookupResult resultInRootPage = lookupInPage(rootPage.getPageId().getPageNumber(), rootPageCount, rootPageBuffer, convertedKeys, 0);

      if (!resultInRootPage.outside) {
        int pageNum = rootPage.getPageId().getPageNumber() + (resultInRootPage.found ? 1 + resultInRootPage.keyIndex : resultInRootPage.keyIndex);
        if (pageNum >= i + rootPageCount)
          // LAST PAGE (AS BOUNDARY), GET PREVIOUS PAGE
          pageNum--;

        final BasePage currentPage = database.getTransaction().getPage(new PageId(file.getFileId(), pageNum), pageSize);
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = getCount(currentPage);

        if (!lookupInPageAndAddInResultset(currentPage, currentPageBuffer, count, convertedKeys, limit, set, removedRIDs))
          return;
      }

      i += rootPageCount;
    }
  }
}
