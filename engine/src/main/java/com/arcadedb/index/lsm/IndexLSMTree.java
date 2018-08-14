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
 * HEADER 1st PAGE = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),bloomFilterSeed(int:4),bloomFilter(bytes[]:<bloomFilterLength>),
 * numberOfKeys(byte:1),keyType(byte:1)*,bfKeyDepth(byte:1)]
 * <p>
 * HEADER Nst PAGE = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),bloomFilterSeed(int:4),bloomFilter(bytes[]:<bloomFilterLength>)]
 */
public class IndexLSMTree extends IndexLSMAbstract {
  public static final String UNIQUE_INDEX_EXT    = "utidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nutidx";

  private final BinaryComparator comparator;
  private final int              bfKeyDepth;

  private AtomicLong statsBFFalsePositive = new AtomicLong();
  private AtomicLong statsAdjacentSteps   = new AtomicLong();

  private static final LookupResult LOWER  = new LookupResult(false, 0, null);
  private static final LookupResult HIGHER = new LookupResult(false, 0, null);

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final String[] propertyNames, final byte[] keyTypes, final int pageSize) throws IOException {
      return new IndexLSMTree(database, name, unique, filePath, mode, propertyNames, keyTypes, pageSize, keyTypes.length);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(Database database, String name, String filePath, int id, PaginatedFile.MODE mode, int pageSize) throws IOException {
      return new IndexLSMTree(database, name, true, filePath, id, mode, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(Database database, String name, String filePath, int id, PaginatedFile.MODE mode, int pageSize) throws IOException {
      return new IndexLSMTree(database, name, false, filePath, id, mode, pageSize);
    }
  }

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
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final String[] propertyNames, final byte[] keyTypes, final int pageSize, final int bfKeyDepth) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    this.comparator = serializer.getComparator();
    this.bfKeyDepth = bfKeyDepth;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at cloning time.
   */
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final String[] propertyNames,
      final byte[] keyTypes, final int pageSize, final int bfKeyDepth) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    this.comparator = serializer.getComparator();
    this.bfKeyDepth = bfKeyDepth;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at load time (1st page only).
   */
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, id, mode, pageSize);
    this.comparator = serializer.getComparator();

    final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);

    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
    this.bfKeyDepth = currentPage.readByte(pos++);
  }

  public IndexLSMTree copy() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
    return new IndexLSMTree(database, newName, unique, database.getDatabasePath() + "/" + newName, null, keyTypes, pageSize, bfKeyDepth);
  }

  @Override
  public void compact() throws IOException {
    if (compacting)
      throw new IllegalStateException("Index '" + name + "' is already compacting");

    compacting = true;
    try {
      final IndexLSMTreeCompactor compactor = new IndexLSMTreeCompactor(this);
      compactor.compact();
    } finally {
      compacting = false;
    }
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) throws IOException {
    return new IndexLSMTreeCursor(this, ascendingOrder);
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
    return new IndexLSMTreeCursor(this, true, fromKeys, toKeys);
  }

  public IndexLSMTreePageIterator newPageIterator(final int pageId, final int currentEntryInPage, final boolean ascendingOrder) throws IOException {
    final BasePage page = database.getTransaction().getPage(new PageId(file.getFileId(), pageId), pageSize);
    return new IndexLSMTreePageIterator(this, page, currentEntryInPage, getHeaderSize(pageId), keyTypes, getCount(page), ascendingOrder);
  }

  @Override
  public void remove(final Object[] keys) {
    internalRemove(keys, null);
  }

  public ModifiablePage appendDuringCompaction(final Binary keyValueContent, ModifiablePage currentPage, TrackableBinary currentPageBuffer, final Object[] keys,
      final RID rid) {
    if (currentPage == null) {

      final int txPageCounter = getTotalPages();

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

    int keyValueFreePosition = getValuesFreePosition(currentPage);

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

    bf.add(BinaryTypes.getHash32(keys, bfKeyDepth));

    setCount(currentPage, count + 1);
    setValuesFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

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
          final List<Object> allValues = readAllValuesFromResult(currentPageBuffer, result);

          // START FROM THE LAST ENTRY
          for (int i = allValues.size() - 1; i > -1; --i) {
            final RID rid = (RID) allValues.get(i);

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

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    stats.put("BFFalsePositive", statsBFFalsePositive.get());
    stats.put("adjacentSteps", statsAdjacentSteps.get());
    return stats;
  }

  /**
   * @param purpose 0 = exists, 1 = retrieve, 2 = ascending iterator, 3 = descending iterator
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
    if (bf.mightContain(BinaryTypes.getHash32(keys, bfKeyDepth))) {
      result = lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, keys, purpose);
      if (!result.found)
        statsBFFalsePositive.incrementAndGet();
    }

    return result;
  }

  /**
   * Lookups for an entry in the index by using dichotomy search.
   *
   * @param purpose 0 = exists, 1 = retrieve, 2 = ascending iterator, 3 = descending iterator
   *
   * @return
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final Binary currentPageBuffer, final Object[] keys, final int purpose) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (keys.length > keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if ((purpose == 0 || purpose == 1) && keys.length != keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if (count == 0)
      // EMPTY, NOT FOUND
      return new LookupResult(false, 0, null);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize(pageNum);

    final Object[] convertedKeys = convertKeys(keys);

    LookupResult result;

    // CHECK THE BOUNDARIES FIRST (LOWER THAN THE FIRST)
    result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, low, count, purpose);
    if (result == LOWER)
      return new LookupResult(false, low, null);
    else if (result != HIGHER)
      return result;

    // CHECK THE BOUNDARIES FIRST (HIGHER THAN THE LAST)
    result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, high, count, purpose);
    if (result == HIGHER)
      return new LookupResult(false, count, null);
    else if (result != LOWER)
      return result;

    while (low <= high) {
      int mid = (low + high) / 2;

      result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, mid, count, purpose);

      if (result == HIGHER)
        low = mid + 1;
      else if (result == LOWER)
        high = mid - 1;
      else
        return result;
    }

    // NOT FOUND
    return new LookupResult(false, low, null);
  }

  @Override
  protected void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot put an entry in the index with a partial key");

    if (unique && checkForUnique) {
      final Set<RID> result = get(keys, 1);
      if (!result.isEmpty())
        throw new DuplicatedKeyException(name, Arrays.toString(keys));
    }

    checkForNulls(keys);

    database.checkTransactionIsActive();

    final int txPageCounter = getTotalPages();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      int count = getCount(currentPage);

      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, unique ? 3 : 0);

      // WRITE KEY/VALUE PAIRS FIRST
      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
      writeEntry(keyValueContent, keys, rid);

      int keyValueFreePosition = getValuesFreePosition(currentPage);

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

      // ADD THE ITEM IN THE BF
      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
          getBFSeed(currentPage));

      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
      bf.add(BinaryTypes.getHash32(keys, bfKeyDepth));

      setCount(currentPage, count + 1);
      setValuesFreePosition(currentPage, keyValueFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
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

      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, 0);
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
                return;
            }

            if (currentRID.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && currentRID.getPosition() == REMOVED_ENTRY_RID.getPosition()) {
              // ALREADY DELETED
              return;
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

      // ADD THE ITEM IN THE BF
      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
          getBFSeed(currentPage));

      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
      bf.add(BinaryTypes.getHash32(keys, bfKeyDepth));

      setCount(currentPage, count + 1);
      setValuesFreePosition(currentPage, keyValueFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
              count + 1, newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
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

  /**
   * Reads the keys and returns the serialized size.
   */
  private int getSerializedKeySize(final Binary buffer, final Object[] keys) {
    final int startsAt = buffer.position();
    for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex)
      serializer.deserializeValue(database, buffer, keyTypes[keyIndex]);

    return buffer.position() - startsAt;
  }

  private Object[] convertKeys(final Object[] keys) {
    final Object[] convertedKeys = new Object[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      if (keys[i] instanceof String)
        convertedKeys[i] = ((String) keys[i]).getBytes();
      else
        convertedKeys[i] = keys[i];
    }
    return convertedKeys;
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

  private LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] keys, final Object[] convertedKeys, int mid,
      final int count, final int purpose) {

    int result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count);

    if (result > 0)
      return HIGHER;
    else if (result < 0)
      return LOWER;

    if (purpose == 0 || purpose == 1) {
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, keys);

      // RETRIEVE ALL THE RESULTS
      final int firstKeyPos = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
      final int lastKeyPos = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);

      final int[] positionsArray = new int[lastKeyPos - firstKeyPos + 1];
      for (int i = firstKeyPos; i <= lastKeyPos; ++i)
        positionsArray[i - firstKeyPos] = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)) + keySerializedSize;

      return new LookupResult(true, lastKeyPos, positionsArray);
    }

    if (convertedKeys.length < keyTypes.length) {
      // PARTIAL MATCHING
      if (purpose == 2) {
        // FIND THE MOST LEFT ITEM
        mid = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
      } else if (purpose == 3) {
        mid = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);
      }
    }

    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
    return new LookupResult(true, mid, new int[] { currentPageBuffer.position() });
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

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    if (pageNum == 0)
      size += BYTE_SERIALIZED_SIZE + keyTypes.length;
    size += BYTE_SERIALIZED_SIZE;
    return size;
  }

  private ModifiablePage createNewPage() {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final ModifiablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);

    int pos = 0;
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    currentPage.writeInt(pos, 0); // ENTRIES COUNT
    pos += INT_SERIALIZED_SIZE;

    // BLOOM FILTER (BF)
    final int seed = new Random(System.currentTimeMillis()).nextInt();

    currentPage.writeInt(pos, seed);
    pos += INT_SERIALIZED_SIZE;
    pos += getBFSize();

    if (txPageCounter == 0) {
      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i)
        currentPage.writeByte(pos++, keyTypes[i]);
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

  protected Object[] readEntryValues(final Binary buffer) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final Object[] rids = new Object[items];

    for (int i = 0; i < rids.length; ++i)
      rids[i] = serializer.deserializeValue(database, buffer, valueType);

    return rids;
  }

  private void readEntryValues(final Binary buffer, final List list) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    final Object[] rids = new Object[items];

    for (int i = 0; i < rids.length; ++i)
      list.add(serializer.deserializeValue(database, buffer, valueType));
  }

  private List<Object> readAllValuesFromResult(final Binary currentPageBuffer, final LookupResult result) {
    final List<Object> allValues = new ArrayList<>();
    for (int i = 0; i < result.valueBeginPositions.length; ++i) {
      currentPageBuffer.position(result.valueBeginPositions[i]);
      readEntryValues(currentPageBuffer, allValues);
    }
    return allValues;
  }

  private int getBFSeed(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  private int getBFSize() {
    return pageSize / 15 / 8 * 8;
  }

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  private void setCount(final ModifiablePage currentPage, final int newCount) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newCount);
  }
}
