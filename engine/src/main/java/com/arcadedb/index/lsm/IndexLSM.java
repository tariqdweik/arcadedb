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
import com.arcadedb.index.IndexException;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * LSM-Tree index. The first page contains 2 bytes to store key and value types. The pages are populated from the head of the page
 * with the pointers to the pair key/value that starts from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (key/value pairs).
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 * <p>
 * HEADER 1st PAGE = [numberOfEntries(int:4),offsetFreeKeyValueContent(int:4),bloomFilterSeed(int:4),
 * bloomFilter(bytes[]:<bloomFilterLength>), numberOfKeys(byte:1),keyType(byte:1)*,valueType(byte:1),bfKeyDepth(byte:1)]
 * <p>
 * HEADER Nst PAGE = [numberOfEntries(int:4),offsetFreeKeyValueContent(int:4),bloomFilterSeed(int:4),
 * bloomFilter(bytes[]:<bloomFilterLength>)]
 */
public class IndexLSM extends PaginatedComponent implements Index {
  public static final String UNIQUE_INDEX_EXT    = "uidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nuidx";
  public static final int    DEF_PAGE_SIZE       = 4 * 1024 * 1024;
  public static final RID    REMOVED_ENTRY_RID   = new RID(null, -1, -1l);

  private final    BinarySerializer serializer;
  private final    BinaryComparator comparator;
  private          byte[]           keyTypes;
  private          byte             valueType;
  private          int              bfKeyDepth;
  private volatile boolean          compacting = false;
  private final    boolean          unique;

  private AtomicLong statsBFFalsePositive = new AtomicLong();
  private AtomicLong statsAdjacentSteps   = new AtomicLong();

  protected class LookupResult {
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
  public IndexLSM(final Database database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode, final byte[] keyTypes,
      final byte valueType, final int pageSize, final int bfKeyDepth) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    this.bfKeyDepth = bfKeyDepth;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at cloning time.
   */
  public IndexLSM(final Database database, final String name, final boolean unique, String filePath, final byte[] keyTypes, final byte valueType,
      final int pageSize, final int bfKeyDepth) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), "temp_" + (unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT),
        PaginatedFile.MODE.READ_WRITE, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    this.bfKeyDepth = bfKeyDepth;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at load time (1st page only).
   */
  public IndexLSM(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();

    final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);

    this.unique = unique;

    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
    this.valueType = currentPage.readByte(pos++);
    this.bfKeyDepth = currentPage.readByte(pos++);
  }

  public IndexLSM copy() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
    return new IndexLSM(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, valueType, pageSize, bfKeyDepth);
  }

  public void removeTempSuffix() {
    // TODO
  }

  @Override
  public void compact() throws IOException {
    if (compacting)
      throw new IllegalStateException("Index '" + name + "' is already compacting");

    compacting = true;
    try {
      final IndexLSMCompactor compactor = new IndexLSMCompactor(this);
      compactor.compact();
    } finally {
      compacting = false;
    }
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) throws IOException {
    return new IndexLSMCursor(this, ascendingOrder);
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
    return new IndexLSMCursor(this, true, fromKeys, toKeys);
  }

  public IndexLSMPageIterator newPageIterator(final int pageId, final int currentEntryInPage, final boolean ascendingOrder) throws IOException {
    final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
    return new IndexLSMPageIterator(this, page, currentEntryInPage, getHeaderSize(pageId), keyTypes, getCount(page), ascendingOrder);
  }

  public boolean isUnique() {
    return unique;
  }

  @Override
  public List<RID> get(final Object[] keys) {
    try {
      final List<RID> list = new ArrayList<>();

      final int totalPages = getTotalPages();

      // SEARCH FROM THE LAST PAGE BACK
      for (int p = totalPages - 1; p > -1; --p) {
        final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), p), pageSize);
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = getCount(currentPage);

        final LookupResult result = searchInPage(currentPage, currentPageBuffer, keys, count, 3);
        if (result != null && result.found) {
          // REAL ALL THE ENTRIES
          final List<Object> allValues = new ArrayList<>();
          for (int i = 0; i < result.valueBeginPositions.length; ++i) {
            currentPageBuffer.position(result.valueBeginPositions[i]);
            readEntryValues(currentPageBuffer, allValues);
          }

          // START FROM THE LAST ENTRY
          boolean exit = false;
          for (int i = allValues.size() - 1; i > -1; --i) {
            RID rid = (RID) allValues.get(i);

            if (rid.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && rid.getPosition() == REMOVED_ENTRY_RID.getPosition()) {
              // DELETED ITEM
              list.clear();
              exit = true;
              break;
            }

            if (rid.getBucketId() < 0)
              // RID DELETED, SKIP THE RID
              continue;

            list.add(rid);

            if (unique) {
              exit = true;
              break;
            }
          }

          if (exit)
            break;
        }
      }

      //LogManager.instance().debug(this, "Get entry by key %s from index '%s' resultItems=%d", Arrays.toString(keys), name, list.size());

      return list;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  @Override
  public void put(final Object[] keys, final RID rid) {
    if (rid == null)
      throw new IllegalArgumentException("RID is null");

    internalPut(keys, rid, true);
  }

  @Override
  public void remove(final Object[] keys) {
    internalRemove(keys, null);
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    internalRemove(keys, rid);
  }

  public ModifiablePage appendDuringCompaction(final Binary keyValueContent, ModifiablePage currentPage, TrackableBinary currentPageBuffer, final Object[] keys,
      final RID rid) {
    if (currentPage == null) {

      Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
      if (txPageCounter == null)
        txPageCounter = pageCount.get();

      try {
        currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), txPageCounter - 1), pageSize, false);
        currentPageBuffer = currentPage.getTrackable();
      } catch (IOException e) {
        throw new DatabaseOperationException("Cannot append key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
      }
    }

    int count = getCount(currentPage);

    int pageNum = currentPage.getPageId().getPageNumber();

    keyValueContent.rewind();

    // MULTI KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      serializer.serializeValue(keyValueContent, keyTypes[i], keys[i]);

    serializer.serializeValue(keyValueContent, valueType, rid);

    int keyValueFreePosition = getKeyValueFreePosition(currentPage);

    if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
      // NO SPACE LEFT, CREATE A NEW PAGE
      database.getTransaction().commit();
      database.getTransaction().begin();
      currentPage = createNewPage();
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

    // ADD THE ITEM IN THE BF
    final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
        getBFSeed(currentPage));

    bf.add(BinaryTypes.getHash(keys, bfKeyDepth));

    setCount(currentPage, count + 1);
    setKeyValueFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    stats.put("BFFalsePositive", statsBFFalsePositive.get());
    stats.put("AdjacentSteps", statsAdjacentSteps.get());
    return stats;
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

  /**
   * @param currentPage
   * @param currentPageBuffer
   * @param keys
   * @param count
   * @param purpose           0 = exists, 1 = ascending iterator, 2 = descending iterator
   *
   * @return
   */
  protected LookupResult searchInPage(final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys, final int count, final int purpose) {
    checkForNulls(keys);

    // SEARCH IN THE BF FIRST
    final int seed = getBFSeed(currentPage);

    final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
        seed);

    LookupResult result = null;
    if (bf.mightContain(BinaryTypes.getHash(keys, bfKeyDepth))) {
      result = lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, keys, purpose);
      if (!result.found)
        statsBFFalsePositive.incrementAndGet();
    }

    return result;
  }

  public int getTotalPages() {
    final Integer txPageCounter = database.getTransaction().getPageCounter(id);
    if (txPageCounter != null)
      return txPageCounter;
    try {
      return (int) database.getFileManager().getVirtualFileSize(file.getFileId()) / pageSize;
    } catch (IOException e) {
      throw new IndexException("Error on determine the total pages", e);
    }
  }

  private int compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object keys[], final int mid, final int count) {
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
        result = comparator.compareStrings((String) key, currentPageBuffer);
      } else {
        final Object keyValue = serializer.deserializeValue(database, currentPageBuffer, keyTypes[keyIndex]);
        result = comparator.compare(key, keyTypes[keyIndex], keyValue, keyTypes[keyIndex]);
      }

      if (result != 0)
        break;
    }

    return result;
  }

  /**
   * Lookups for an entry in the index by using dichotomic search.
   *
   * @param purpose 0 = exists, 1 = ascending iterator, 2 = descending iterator, 3 = retrieve
   *
   * @return
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final Binary currentPageBuffer, final Object[] keys, final int purpose) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (keys.length > keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if ((purpose == 0 || purpose == 3) && keys.length != keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if (count == 0)
      // EMPTY, NOT FOUND
      return new LookupResult(false, 0, null);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize(pageNum);

    while (low <= high) {
      int mid = (low + high) / 2;

      int result = compareKey(currentPageBuffer, startIndexArray, keys, mid, count);

      if (result > 0) {
        low = mid + 1;
        continue;
      } else if (result < 0) {
        high = mid - 1;
        continue;
      }

      if (purpose == 3) {
        currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
        final int keySerializedSize = getSerializedKeySize(currentPageBuffer, keys);

        // RETRIEVE ALL THE RESULTS
        final int firstKeyPos = findFirstEntryOfSameKey(currentPageBuffer, keys, startIndexArray, mid);
        final int lastKeyPos = findLastEntryOfSameKey(count, currentPageBuffer, keys, startIndexArray, mid);

        final int[] positionsArray = new int[lastKeyPos - firstKeyPos + 1];
        for (int i = firstKeyPos; i <= lastKeyPos; ++i)
          positionsArray[i - firstKeyPos] = currentPageBuffer.getInt(startIndexArray + (firstKeyPos * INT_SERIALIZED_SIZE)) + keySerializedSize;

        return new LookupResult(true, firstKeyPos, positionsArray);
      }

      if (keys.length < keyTypes.length) {
        // PARTIAL MATCHING
        if (purpose == 1) {
          // FIND THE MOST LEFT ITEM
          mid = findFirstEntryOfSameKey(currentPageBuffer, keys, startIndexArray, mid);
        } else if (purpose == 2) {
          mid = findLastEntryOfSameKey(count, currentPageBuffer, keys, startIndexArray, mid);
        }
      }

      // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
      return new LookupResult(true, mid, new int[] { currentPageBuffer.position() });
    }

    // NOT FOUND
    return new LookupResult(false, low, null);

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
          result = comparator.compareStrings((String) keys[keyIndex], currentPageBuffer);
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
          result = comparator.compareStrings((String) keys[keyIndex], currentPageBuffer);
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

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    if (pageNum == 0)
      size += BYTE_SERIALIZED_SIZE + keyTypes.length + BYTE_SERIALIZED_SIZE;
    size += BYTE_SERIALIZED_SIZE;
    return size;
  }

  private ModifiablePage createNewPage() {
    // NEW FILE, CREATE HEADER PAGE
    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount.get();

    final ModifiablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);

    int pos = 0;
    currentPage.writeInt(pos, 0); // ENTRIES COUNT
    pos += INT_SERIALIZED_SIZE;
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    // BLOOM FILTER (BF)
    final int seed = new Random(System.currentTimeMillis()).nextInt();

    currentPage.writeInt(pos, seed);
    pos += INT_SERIALIZED_SIZE;
    pos += getBFSize();

    if (pageCount.get() == 0) {
      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i) {
        currentPage.writeByte(pos++, keyTypes[i]);
      }
      currentPage.writeByte(pos++, valueType);
      currentPage.writeByte(pos++, (byte) bfKeyDepth);
    }

    return currentPage;
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

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(0);
  }

  private void setCount(final ModifiablePage currentPage, final int newCount) {
    currentPage.writeInt(0, newCount);
  }

  protected int getBFSeed(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  protected Object[] checkForNulls(final Object keys[]) {
    if (keys != null)
      for (int i = 0; i < keys.length; ++i)
        if (keys[i] == null)
          throw new IllegalArgumentException("Indexed key cannot be NULL");
    return keys;
  }

  private int getKeyValueFreePosition(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  private void setKeyValueFreePosition(final ModifiablePage currentPage, final int newKeyValueFreePosition) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newKeyValueFreePosition);
  }

  private int getBFSize() {
    return pageSize / 15 / 8 * 8;
  }

  private void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot put an entry in the index with a partial key");

    checkForNulls(keys);

    if (unique && checkForUnique) {
      final List<RID> result = get(keys);
      if (!result.isEmpty())
        throw new DuplicatedKeyException(name, Arrays.toString(keys));
    }

    database.checkTransactionIsActive();

    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount.get();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      int count = getCount(currentPage);

      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, 0);
      if (unique && checkForUnique && result.found)
        throw new DuplicatedKeyException(name, Arrays.toString(keys));

      // WRITE KEY/VALUE PAIRS FIRST
      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
      writeEntry(keyValueContent, keys, rid);

      int keyValueFreePosition = getKeyValueFreePosition(currentPage);

      int keyIndex = result.keyIndex;
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

      // SHIFT POINTERS TO THE RIGHT
      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
      currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

      currentPageBuffer.putInt(startPos, keyValueFreePosition);

      // ADD THE ITEM IN THE BF
      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
          getBFSeed(currentPage));

      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
      bf.add(BinaryTypes.getHash(keys, bfKeyDepth));

      setCount(currentPage, count + 1);
      setKeyValueFreePosition(currentPage, keyValueFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  private void internalRemove(final Object[] keys, final RID rid) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");

    checkForNulls(keys);

    database.checkTransactionIsActive();

    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount.get();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      int count = getCount(currentPage);

      final RID removedRID = rid != null ? getRemovedRID(rid) : REMOVED_ENTRY_RID;

      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, 0);
      if (result.found) {
        // LAST PAGE IS NOT IMMUTABLE (YET), UPDATE THE 1ST VALUE
        currentPageBuffer.position(result.valueBeginPositions[0]);

        if (rid != null) {
          // SEARCH FOR THE VALUE TO REPLACE
          final Object[] values = readEntryValues(currentPageBuffer);
          for (int i = 0; i < values.length; ++i) {
            if (rid.equals(values[i])) {
              updateEntryValue(currentPageBuffer, i, removedRID);
              return;
            }
          }
        }
      }

      // WRITE KEY/VALUE PAIRS FIRST
      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
      writeEntry(keyValueContent, keys, removedRID);

      int keyValueFreePosition = getKeyValueFreePosition(currentPage);

      int keyIndex = result.keyIndex;
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

      // SHIFT POINTERS TO THE RIGHT
      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
      currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

      currentPageBuffer.putInt(startPos, keyValueFreePosition);

      // ADD THE ITEM IN THE BF
      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
          getBFSeed(currentPage));

      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
      bf.add(BinaryTypes.getHash(keys, bfKeyDepth));

      setCount(currentPage, count + 1);
      setKeyValueFreePosition(currentPage, keyValueFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  private RID getRemovedRID(final RID rid) {
    return new RID(database, (rid.getBucketId() + 2) * -1, rid.getPosition());
  }

  private RID getOriginalRID(final RID rid) {
    return new RID(database, (rid.getBucketId() * -1) - 2, rid.getPosition());
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

  private void writeEntry(final Binary buffer, final Object[] keys, final Object rid) {
    buffer.clear();

    // WRITE KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      serializer.serializeValue(buffer, keyTypes[i], keys[i]);

    writeEntryValue(buffer, rid);
  }

  private void writeEntry(final Binary buffer, final Object[] keys, final Object[] rids) {
    buffer.clear();

    // WRITE KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      serializer.serializeValue(buffer, keyTypes[i], keys[i]);

    writeEntryValues(buffer, rids);
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

  private void updateEntryValue(final Binary buffer, final int valueIndex, final Object value) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    if (valueIndex > items - 1)
      throw new IllegalArgumentException("Cannot update value index " + valueIndex + " in value container with only " + items + " items");

    // MOVE TO THE LAST ITEM
    buffer.position(buffer.position() + (BinaryTypes.getTypeSize(valueType) * valueIndex));

    // WRITE VALUES
    serializer.serializeValue(buffer, valueType, value);
  }

  /**
   * Reads the keys and returns the serialized size.
   */
  private int getSerializedKeySize(final Binary buffer, final Object[] keys) {
    final int startsAt = buffer.position();
    for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex)
      serializer.deserializeValue(database, buffer, keyTypes[keyIndex]);

    return buffer.position() - startsAt;
  }
}
