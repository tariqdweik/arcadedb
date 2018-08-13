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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * LSM-Hash index. The first page contains 2 bytes to store key and value types. The pages are populated from the head of the page
 * with the hash+pointer entries to the values that starts from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (values).
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 * <p>
 * HEADER 1st PAGE = [numberOfEntries(int:4),offsetFreeValueContent(int:4),numberOfKeys(byte:1),keyType(byte:1)*,valueType(byte:1)]
 * <p>
 * HEADER Nst PAGE = [numberOfEntries(int:4),offsetFreeValueContent(int:4)
 */
public class IndexLSMHash extends IndexLSMAbstract {
  public static final String UNIQUE_INDEX_EXT    = "uhidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nuhidx";

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize) throws IOException {
      return new IndexLSMHash(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(Database database, String name, String filePath, int id, PaginatedFile.MODE mode, int pageSize) throws IOException {
      return new IndexLSMHash(database, name, true, filePath, id, mode, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(Database database, String name, String filePath, int id, PaginatedFile.MODE mode, int pageSize) throws IOException {
      return new IndexLSMHash(database, name, false, filePath, id, mode, pageSize);
    }
  }

  /**
   * Called at creation time.
   */
  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at cloning time.
   */
  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final byte[] keyTypes, final int pageSize)
      throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at load time (1st page only).
   */
  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, id, mode, pageSize);

    final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);

    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
    this.valueType = currentPage.readByte(pos++); // RID
  }

  public IndexLSMHash copy() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
    return new IndexLSMHash(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, pageSize);
  }

  @Override
  public void compact() throws IOException {
    if (compacting)
      throw new IllegalStateException("Index '" + name + "' is already compacting");

    compacting = true;
    try {
// TODO
//      final IndexLSMCompactor compactor = new IndexLSMCompactor(this);
//      compactor.compact();
    } finally {
      compacting = false;
    }
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) {
    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys) {
    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
  }

  @Override
  public IndexCursor range(final Object[] fromKeys, final Object[] toKeys) {
    throw new UnsupportedOperationException("LSM-Hash index does not allow range operation");
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    stats.put("conflictedEntriesFound", 0l);
    return stats;
  }

  /**
   * @param purpose 0 = exists, 1 = retrieve
   *
   * @return
   */
  protected LookupResult searchInPage(final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys, final int count, final int purpose) {
    checkForNulls(keys);
    return lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPage, currentPageBuffer, keys);
  }

  /**
   * Lookups for an entry in the index. The result must be filtered because for the nature of the hashing, it may contain non related entries in the same
   * container because conflicted keys.
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (keys.length > keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if (keys.length < keyTypes.length)
      throw new IllegalArgumentException("Partial key matching not supported by lsm-hash index");

    if (count > 0) {
      final int startIndexArray = getHeaderSize(pageNum);

      final int hashedKey = BinaryTypes.getHash32(keys);
      final int entryIndexPosition = hashedKey % getCount(currentPage);

      final int pos = currentPageBuffer.getInt(startIndexArray + (entryIndexPosition * INT_SERIALIZED_SIZE));
      if (pos > 0)
        // FOUND
        return new LookupResult(true, 0, new int[] { pos });
    }

    // NOT FOUND
    return new LookupResult(false, 0, null);
  }

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    if (pageNum == 0)
      size += BYTE_SERIALIZED_SIZE + keyTypes.length + BYTE_SERIALIZED_SIZE;
    size += BYTE_SERIALIZED_SIZE;
    return size;
  }

  private ModifiablePage createNewPage() {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final ModifiablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);

    int pos = 0;
    currentPage.writeInt(pos, pageSize / 4); // ENTRIES COUNT, BY DEFAULT 1/4 OF THE PAGE SIZE
    pos += INT_SERIALIZED_SIZE;
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    if (pageCount.get() == 0) {
      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i) {
        currentPage.writeByte(pos++, keyTypes[i]);
      }
      currentPage.writeByte(pos++, valueType);
    }

    return currentPage;
  }

  @Override
  protected void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot put an entry in the index with a partial key");

    checkForNulls(keys);

    if (unique && checkForUnique) {
      final Set<RID> result = get(keys, 1);
      if (!result.isEmpty())
        throw new DuplicatedKeyException(name, Arrays.toString(keys));
    }

    database.checkTransactionIsActive();

    final int txPageCounter = getTotalPages();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      int count = getCount(currentPage);

      final LookupResult result = lookupInPage(pageNum, count, currentPage, currentPageBuffer, keys);
      if (unique && checkForUnique && result.found)
        checkUniqueConstraint(keys, currentPageBuffer, result);

      // WRITE VALUE FIRST
      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
      writeEntryValue(keyValueContent, rid);

      // ADD ONE BYTE IN CASE THE CONTAINER LENGTH NEED TO USE ANOTHER BYTE AS VARINT
      final int maxPositionForThisValue = keyValueContent.size() + 1;

      int entriesFreePosition = getEntriesFreePosition(currentPage);

      boolean newPage = false;
      if (entriesFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE)) < maxPositionForThisValue) {
        // NO SPACE LEFT, CREATE A NEW PAGE
        newPage = true;

        currentPage = createNewPage();
        currentPageBuffer = currentPage.getTrackable();
        pageNum = currentPage.getPageId().getPageNumber();
        count = 0;
        entriesFreePosition = currentPage.getMaxContentSize();
      }

      if (result.found) {
        // UPDATE THE EXISTING CONTAINER IF THERE IS ROOM
        currentPageBuffer.position(result.valueBeginPositions[0]);
        addEntryValue(currentPageBuffer, rid);
      }

      entriesFreePosition -= keyValueContent.size();

      // WRITE KEY/VALUE PAIR CONTENT
      currentPageBuffer.putByteArray(entriesFreePosition, keyValueContent.toByteArray());
//
//      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
//      if (keyIndex < count)
//        // NOT LAST KEY, SHIFT POINTERS TO THE RIGHT
//        currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);
//
//      currentPageBuffer.putInt(startPos, entriesFreePosition);
//
//      // ADD THE ITEM IN THE BF
//      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
//          getBFSeed(currentPage));
//
//      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
//      bf.add(BinaryTypes.getHash32(keys, bfKeyDepth));
//
//      setCount(currentPage, count + 1);
//      setEntriesFreePosition(currentPage, entriesFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  protected void addEntryValue(final Binary buffer, final Object value) {
//    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);
//
//    if (valueIndex > items - 1)
//      throw new IllegalArgumentException("Cannot update value index " + valueIndex + " in value container with only " + items + " items");
//
//    // MOVE TO THE LAST ITEM
//    buffer.position(buffer.position() + (BinaryTypes.getTypeSize(valueType) * valueIndex));
//
//    // WRITE VALUES
//    serializer.serializeValue(buffer, valueType, value);
  }

  @Override
  protected void internalRemove(final Object[] keys, final RID rid) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");

    checkForNulls(keys);

    database.checkTransactionIsActive();

    final int txPageCounter = getTotalPages();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      int count = getCount(currentPage);

      final RID removedRID = rid != null ? getRemovedRID(rid) : REMOVED_ENTRY_RID;

      final LookupResult result = lookupInPage(pageNum, count, currentPage, currentPageBuffer, keys);
      if (result.found) {
        // LAST PAGE IS NOT IMMUTABLE (YET), UPDATE THE 1ST VALUE
        currentPageBuffer.position(result.valueBeginPositions[0]);

        if (rid != null) {
          // SEARCH FOR THE VALUE TO REPLACE
          final Object[] values = readEntryValues(currentPageBuffer);
          for (int i = 0; i < values.length; ++i) {
            if (rid.equals(values[i])) {
              // OVERWRITE LAST VALUE
              currentPageBuffer.position(result.valueBeginPositions[result.valueBeginPositions.length - 1]);
              updateEntryValue(currentPageBuffer, i, removedRID);
              return;
            } else if (removedRID.equals(values[i]))
              // ALREADY DELETED
              return;
          }
        }
      }

      // WRITE KEY/VALUE PAIRS FIRST
      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
//      writeEntry(keyValueContent, keys, removedRID);

      int keyValueFreePosition = getEntriesFreePosition(currentPage);

      int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;
      boolean newPage = false;
      if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
        // NO SPACE LEFT, CREATE A NEW PAGE
        newPage = true;

        currentPage = createNewPage();
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

      //setCount(currentPage, count + 1);
      setEntriesFreePosition(currentPage, keyValueFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }
}
