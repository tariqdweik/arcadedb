/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

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
//public class IndexLSMHash extends IndexLSMAbstract {
//  public static final String UNIQUE_INDEX_EXT    = "uhidx";
//  public static final String NOTUNIQUE_INDEX_EXT = "nuhidx";
//  public static final int    DEF_PAGE_SIZE       = 4 * 1024 * 1024;
//
//  /**
//   * Called at creation time.
//   */
//  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode, final byte[] keyTypes,
//      final byte valueType, final int pageSize) throws IOException {
//    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, valueType, pageSize);
//    database.checkTransactionIsActive();
//    createNewPage();
//  }
//
//  /**
//   * Called at cloning time.
//   */
//  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final byte[] keyTypes, final byte valueType,
//      final int pageSize) throws IOException {
//    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, valueType, pageSize);
//    database.checkTransactionIsActive();
//    createNewPage();
//  }
//
//  /**
//   * Called at load time (1st page only).
//   */
//  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
//      final int pageSize) throws IOException {
//    super(database, name, unique, filePath, id, mode, pageSize);
//
//    final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);
//
//    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
//    final int len = currentPage.readByte(pos++);
//    this.keyTypes = new byte[len];
//    for (int i = 0; i < len; ++i)
//      this.keyTypes[i] = currentPage.readByte(pos++);
//    this.valueType = currentPage.readByte(pos++); // RID
//  }
//
//  public IndexLSMHash copy() throws IOException {
//    int last_ = name.lastIndexOf('_');
//    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
//    return new IndexLSMHash(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, valueType, pageSize);
//  }
//
//  @Override
//  public void compact() throws IOException {
//    if (compacting)
//      throw new IllegalStateException("Index '" + name + "' is already compacting");
//
//    compacting = true;
//    try {
//// TODO
////      final IndexLSMCompactor compactor = new IndexLSMCompactor(this);
////      compactor.compact();
//    } finally {
//      compacting = false;
//    }
//  }
//
//  @Override
//  public IndexCursor iterator(final boolean ascendingOrder) {
//    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
//  }
//
//  @Override
//  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) {
//    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
//  }
//
//  @Override
//  public IndexCursor iterator(final Object[] fromKeys) {
//    throw new UnsupportedOperationException("LSM-Hash index cannot be iterated");
//  }
//
//  @Override
//  public IndexCursor range(final Object[] fromKeys, final Object[] toKeys) {
//    throw new UnsupportedOperationException("LSM-Hash index does not allow range operation");
//  }
//
//  /**
//   * @param purpose 0 = exists, 1 = ascending iterator, 2 = descending iterator
//   *
//   * @return
//   */
//  protected LookupResult searchInPage(final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys, final int count, final int purpose) {
//    checkForNulls(keys);
//    return lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, keys, purpose);
//  }
//
//  /**
//   * Lookups for an entry in the index by using dichotomic search.
//   *
//   * @param purpose 0 = exists, 1 = ascending iterator, 2 = descending iterator, 3 = retrieve
//   *
//   * @return
//   */
//  protected LookupResult lookupInPage(final int pageNum, final int count, final Binary currentPageBuffer, final Object[] keys, final int purpose) {
//    if (keyTypes.length == 0)
//      throw new IllegalArgumentException("No key types found");
//
//    if (keys.length > keyTypes.length)
//      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");
//
//    if ((purpose == 0 || purpose == 3) && keys.length != keyTypes.length)
//      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");
//
//    if (count == 0)
//      // EMPTY, NOT FOUND
//      return new LookupResult(false, 0, null);
//
//    int low = 0;
//    int high = count - 1;
//
//    final int startIndexArray = getHeaderSize(pageNum);
//
//    final Object[] convertedKeys = convertKeys(keys);
//
//    LookupResult result;
//
//    // CHECK THE BOUNDARIES FIRST (LOWER THAN THE FIRST)
//    result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, low, count, purpose);
//    if (result == LOWER)
//      return new LookupResult(false, low, null);
//    else if (result != HIGHER)
//      return result;
//
//    // CHECK THE BOUNDARIES FIRST (HIGHER THAN THE LAST)
//    result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, high, count, purpose);
//    if (result == HIGHER)
//      return new LookupResult(false, count, null);
//    else if (result != LOWER)
//      return result;
//
//    while (low <= high) {
//      int mid = (low + high) / 2;
//
//      result = compareKey(currentPageBuffer, startIndexArray, keys, convertedKeys, mid, count, purpose);
//
//      if (result == HIGHER)
//        low = mid + 1;
//      else if (result == LOWER)
//        high = mid - 1;
//      else
//        return result;
//    }
//
//    // NOT FOUND
//    return new LookupResult(false, low, null);
//  }
//
//  private LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] keys, final Object[] convertedKeys, int mid,
//      final int count, final int purpose) {
//
//    int result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count);
//
//    if (result > 0)
//      return HIGHER;
//    else if (result < 0)
//      return LOWER;
//
//    if (purpose == 3) {
//      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
//      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, keys);
//
//      // RETRIEVE ALL THE RESULTS
//      final int firstKeyPos = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
//      final int lastKeyPos = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);
//
//      final int[] positionsArray = new int[lastKeyPos - firstKeyPos + 1];
//      for (int i = firstKeyPos; i <= lastKeyPos; ++i)
//        positionsArray[i - firstKeyPos] = currentPageBuffer.getInt(startIndexArray + (firstKeyPos * INT_SERIALIZED_SIZE)) + keySerializedSize;
//
//      return new LookupResult(true, lastKeyPos, positionsArray);
//    }
//
//    if (convertedKeys.length < keyTypes.length) {
//      // PARTIAL MATCHING
//      if (purpose == 1) {
//        // FIND THE MOST LEFT ITEM
//        mid = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
//      } else if (purpose == 2) {
//        mid = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);
//      }
//    }
//
//    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
//    return new LookupResult(true, mid, new int[] { currentPageBuffer.position() });
//  }
//
//  private int getHeaderSize(final int pageNum) {
//    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
//    if (pageNum == 0)
//      size += BYTE_SERIALIZED_SIZE + keyTypes.length + BYTE_SERIALIZED_SIZE;
//    size += BYTE_SERIALIZED_SIZE;
//    return size;
//  }
//
//  private ModifiablePage createNewPage() {
//    // NEW FILE, CREATE HEADER PAGE
//    final int txPageCounter = getTotalPages();
//
//    final ModifiablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);
//
//    int pos = 0;
//    currentPage.writeInt(pos, 0); // ENTRIES COUNT
//    pos += INT_SERIALIZED_SIZE;
//    currentPage.writeInt(pos, currentPage.getMaxContentSize());
//    pos += INT_SERIALIZED_SIZE;
//
//    // BLOOM FILTER (BF)
//    final int seed = new Random(System.currentTimeMillis()).nextInt();
//
//    currentPage.writeInt(pos, seed);
//    pos += INT_SERIALIZED_SIZE;
//
//    if (pageCount.get() == 0) {
//      currentPage.writeByte(pos++, (byte) keyTypes.length);
//      for (int i = 0; i < keyTypes.length; ++i) {
//        currentPage.writeByte(pos++, keyTypes[i]);
//      }
//      currentPage.writeByte(pos++, valueType);
//    }
//
//    return currentPage;
//  }
//
//  @Override
//  protected void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
//    if (keys.length != keyTypes.length)
//      throw new IllegalArgumentException("Cannot put an entry in the index with a partial key");
//
//    checkForNulls(keys);
//
//    if (unique && checkForUnique) {
//      final Set<RID> result = get(keys);
//      if (!result.isEmpty())
//        throw new DuplicatedKeyException(name, Arrays.toString(keys));
//    }
//
//    database.checkTransactionIsActive();
//
//    final int txPageCounter = getTotalPages();
//
//    int pageNum = txPageCounter - 1;
//
//    try {
//      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);
//
//      TrackableBinary currentPageBuffer = currentPage.getTrackable();
//
//      int count = getCount(currentPage);
//
//      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, unique ? 3 : 0);
//      if (unique && checkForUnique && result.found) {
//        // CHECK FOR DUPLICATES
//        final List<Object> allValues = readAllValues(currentPageBuffer, result);
//
//        final Set<RID> removedRIDs = new HashSet<>();
//
//        for (int i = allValues.size() - 1; i > -1; --i) {
//          final RID valueAsRid = (RID) allValues.get(i);
//          if (valueAsRid.getBucketId() == REMOVED_ENTRY_RID.getBucketId() && valueAsRid.getPosition() == REMOVED_ENTRY_RID.getPosition())
//            // DELETED ITEM, FINE
//            break;
//
//          if (valueAsRid.getBucketId() < 0) {
//            // RID DELETED, SKIP THE RID
//            removedRIDs.add(getOriginalRID(valueAsRid));
//            continue;
//          }
//
//          if (removedRIDs.contains(valueAsRid))
//            // ALREADY FOUND AS DELETED, FINE
//            continue;
//
//          throw new DuplicatedKeyException(name, Arrays.toString(keys));
//        }
//      }
//
//      // TODO: OPTIMIZATION: REPLACE SAME KEY/RID (even deleted) INSTEAD OF ADDING A NEW ENTRY
//
//      // WRITE KEY/VALUE PAIRS FIRST
//      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
//      writeEntry(keyValueContent, keys, rid);
//
//      int keyValueFreePosition = getKeyValueFreePosition(currentPage);
//
//      int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;
//      boolean newPage = false;
//      if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
//        // NO SPACE LEFT, CREATE A NEW PAGE
//        newPage = true;
//
//        currentPage = createNewPage();
//        currentPageBuffer = currentPage.getTrackable();
//        pageNum = currentPage.getPageId().getPageNumber();
//        count = 0;
//        keyIndex = 0;
//        keyValueFreePosition = currentPage.getMaxContentSize();
//      }
//
//      keyValueFreePosition -= keyValueContent.size();
//
//      // WRITE KEY/VALUE PAIR CONTENT
//      currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());
//
//      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
//      if (keyIndex < count)
//        // NOT LAST KEY, SHIFT POINTERS TO THE RIGHT
//        currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);
//
//      currentPageBuffer.putInt(startPos, keyValueFreePosition);
//
//      // ADD THE ITEM IN THE BF
//      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
//          getBFSeed(currentPage));
//
//      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
//      bf.add(BinaryTypes.getHash(keys, bfKeyDepth));
//
//      setCount(currentPage, count + 1);
//      setKeyValueFreePosition(currentPage, keyValueFreePosition);
//
//      LogManager.instance()
//          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
//              count + 1, newPage);
//
//    } catch (IOException e) {
//      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
//    }
//  }
//
//  @Override
//  protected void internalRemove(final Object[] keys, final RID rid) {
//    if (keys.length != keyTypes.length)
//      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");
//
//    checkForNulls(keys);
//
//    database.checkTransactionIsActive();
//
//    final int txPageCounter = getTotalPages();
//
//    int pageNum = txPageCounter - 1;
//
//    try {
//      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);
//
//      TrackableBinary currentPageBuffer = currentPage.getTrackable();
//
//      int count = getCount(currentPage);
//
//      final RID removedRID = rid != null ? getRemovedRID(rid) : REMOVED_ENTRY_RID;
//
//      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, 0);
//      if (result.found) {
//        // LAST PAGE IS NOT IMMUTABLE (YET), UPDATE THE 1ST VALUE
//        currentPageBuffer.position(result.valueBeginPositions[0]);
//
//        if (rid != null) {
//          // SEARCH FOR THE VALUE TO REPLACE
//          final Object[] values = readEntryValues(currentPageBuffer);
//          for (int i = 0; i < values.length; ++i) {
//            if (rid.equals(values[i])) {
//              // OVERWRITE LAST VALUE
//              currentPageBuffer.position(result.valueBeginPositions[result.valueBeginPositions.length - 1]);
//              updateEntryValue(currentPageBuffer, i, removedRID);
//              return;
//            } else if (removedRID.equals(values[i]))
//              // ALREADY DELETED
//              return;
//          }
//        }
//      }
//
//      // WRITE KEY/VALUE PAIRS FIRST
//      final Binary keyValueContent = database.getContext().getTemporaryBuffer1();
//      writeEntry(keyValueContent, keys, removedRID);
//
//      int keyValueFreePosition = getKeyValueFreePosition(currentPage);
//
//      int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;
//      boolean newPage = false;
//      if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
//        // NO SPACE LEFT, CREATE A NEW PAGE
//        newPage = true;
//
//        currentPage = createNewPage();
//        currentPageBuffer = currentPage.getTrackable();
//        pageNum = currentPage.getPageId().getPageNumber();
//        count = 0;
//        keyIndex = 0;
//        keyValueFreePosition = currentPage.getMaxContentSize();
//      }
//
//      keyValueFreePosition -= keyValueContent.size();
//
//      // WRITE KEY/VALUE PAIR CONTENT
//      currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());
//
//      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
//      if (keyIndex < count)
//        // NOT LAST KEY, SHIFT POINTERS TO THE RIGHT
//        currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);
//
//      currentPageBuffer.putInt(startPos, keyValueFreePosition);
//
//      // ADD THE ITEM IN THE BF
//      final BufferBloomFilter bf = new BufferBloomFilter(currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
//          getBFSeed(currentPage));
//
//      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
//      bf.add(BinaryTypes.getHash(keys, bfKeyDepth));
//
//      setCount(currentPage, count + 1);
//      setKeyValueFreePosition(currentPage, keyValueFreePosition);
//
//      LogManager.instance()
//          .debug(this, "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)", Arrays.toString(keys), rid, name, currentPage.getPageId(),
//              count + 1, newPage);
//
//    } catch (IOException e) {
//      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
//    }
//  }
//}
