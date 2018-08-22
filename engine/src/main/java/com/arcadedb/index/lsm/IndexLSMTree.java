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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.LockContext;
import com.arcadedb.utility.LogManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
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
 * HEADER ROOT PAGES (1st) = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4),compactedPages(int:4),subIndexFileId(int:4),numberOfKeys(byte:1),keyType(byte:1)*]
 * <p>
 * HEADER Nst PAGE         = [offsetFreeKeyValueContent(int:4),numberOfEntries(int:4)]
 */
public class IndexLSMTree extends IndexLSMAbstract {
  public static final String UNIQUE_INDEX_EXT    = "utidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nutidx";

  private final    BinaryComparator comparator;
  private          int              subIndexFileId    = -1;
  private          IndexLSMTree     subIndex          = null;
  private          LockContext      lock              = new LockContext();
  private volatile boolean          dropWhenCollected = false;

  private AtomicLong statsAdjacentSteps = new AtomicLong();

  private static final LookupResult LOWER  = new LookupResult(false, true, 0, null);
  private static final LookupResult HIGHER = new LookupResult(false, true, 0, null);

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize) throws IOException {
      return new IndexLSMTree(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      return new IndexLSMTree(database, name, true, filePath, id, mode, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent create(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      return new IndexLSMTree(database, name, false, filePath, id, mode, pageSize);
    }
  }

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
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    this.comparator = serializer.getComparator();
    database.checkTransactionIsActive();
    createNewPage(0);
  }

  /**
   * Called at cloning time.
   */
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final byte[] keyTypes, final int pageSize,
      final COMPACTING_STATUS compactingStatus, final IndexLSMTree subIndex) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    this.comparator = serializer.getComparator();
    this.compactingStatus = compactingStatus;
    this.subIndex = subIndex;
    database.checkTransactionIsActive();
  }

  /**
   * Called at load time (1st page only).
   */
  public IndexLSMTree(final Database database, final String name, final boolean unique, final String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, id, mode, pageSize);
    this.comparator = serializer.getComparator();

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
    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (dropWhenCollected) {
          try {
            LogManager.instance().info(this, "Finalizing deletion of index '%s'...", name);
            if (database.isOpen())
              ((SchemaImpl) database.getSchema()).removeIndex(getName());
            drop();
          } catch (IOException e) {
            LogManager.instance().error(this, "Error on dropping the index '%s'", e, name);
          }
        }

        if (subIndex != null)
          subIndex.close();
        return null;
      }
    });
  }

  @Override
  public void onAfterLoad() {
    if (subIndexFileId > -1)
      subIndex = (IndexLSMTree) database.getSchema().getFileById(subIndexFileId);
  }

  public IndexLSMTree createNewForCompaction() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();

    return new IndexLSMTree(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, pageSize, COMPACTING_STATUS.COMPACTED, null);
  }

  @Override
  public boolean compact() throws IOException {
    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + name + "'");

    if (compactingStatus == COMPACTING_STATUS.IN_PROGRESS)
      throw new IllegalStateException("Index '" + name + "' is already compacting");

    compactingStatus = COMPACTING_STATUS.IN_PROGRESS;
    try {
      final IndexLSMTreeCompactor compactor = new IndexLSMTreeCompactor(this);
      return compactor.compact();
    } finally {
      compactingStatus = COMPACTING_STATUS.COMPACTED;
    }
  }

  @Override
  public void finalize() {
    close();
  }

  public void lazyDrop() {
    dropWhenCollected = true;
  }

  public void copyPagesToNewFile(final int startingFromPage, final IndexLSMTree subIndex) throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();

    final IndexLSMTree newIndex = new IndexLSMTree(database, newName, unique, database.getDatabasePath() + "/" + newName, keyTypes, pageSize,
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
        ((SchemaImpl) database.getSchema()).swapIndexes(IndexLSMTree.this, newIndex);

        return null;
      }
    });
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

  public IndexLSMTree getSubIndex() {
    return subIndex;
  }

  @Override
  public void remove(final Object[] keys) {
    internalRemove(keys, null);
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    final Object[] convertedKeys = convertKeys(keys, keyTypes);

    try {
      final Set<RID> set = new HashSet<>();

      final Set<RID> removedRIDs = new HashSet<>();

      if (compactingStatus == COMPACTING_STATUS.COMPACTED)
        // SEARCH IN COMPACTED INDEX
        searchInCompactedIndex(convertedKeys, limit, set, removedRIDs);
      else
        // NON COMPACTED INDEX, SEARCH IN ALL THE PAGES
        searchInNonCompactedIndex(convertedKeys, limit, set, removedRIDs);

      return set;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("pages", (long) getTotalPages());
    stats.put("adjacentSteps", statsAdjacentSteps.get());
    return stats;
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

    if (subIndex != null)
      subIndex.removeTempSuffix();
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

  @Override
  protected void internalPut(final Object[] keys, final RID rid, final boolean checkForUnique) {
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
      public Object call() throws Exception {
        final int txPageCounter = getTotalPages();

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

  @Override
  protected void internalRemove(final Object[] keys, final RID rid) {
    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + name + "'");

    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");

    checkForNulls(keys);

    database.checkTransactionIsActive();

    lock.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final int txPageCounter = getTotalPages();

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

  public MutablePage appendDuringCompaction(final Binary keyValueContent, MutablePage currentPage, TrackableBinary currentPageBuffer, final int pagesToCompact,
      final Object[] keys, final RID[] rids) {
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
  private int getSerializedKeySize(final Binary buffer, final int keyLength) {
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

  private LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] convertedKeys, int mid, final int count,
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

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    if (pageNum == 0)
      size += INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + keyTypes.length;

    return size;
  }

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

      currentPage.writeInt(pos, subIndex != null ? subIndex.id : -1); // SUB-INDEX FILE ID
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i)
        currentPage.writeByte(pos++, keyTypes[i]);
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

  protected int getCompactedPages(final BasePage currentPage) {
    if (currentPage.getPageId().getPageNumber() != 0)
      throw new IllegalArgumentException("Compacted pages information is stored only on the 1st page");

    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  protected int getSubIndex(final BasePage currentPage) {
    if (currentPage.getPageId().getPageNumber() != 0)
      throw new IllegalArgumentException("Sub-index information is stored only on the 1st page");

    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  protected int getCount(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  private void setCount(final MutablePage currentPage, final int newCount) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newCount);
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

  private void searchInCompactedIndex(final Object[] convertedKeys, final int limit, final Set<RID> set, final Set<RID> removedRIDs) throws IOException {
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
        if (pageNum >= getTotalPages())
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

  private boolean lookupInPageAndAddInResultset(final BasePage currentPage, final Binary currentPageBuffer, final int count, final Object[] convertedKeys,
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
}
