/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.*;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.*;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * LSM-Hash index. The first page contains 2 bytes to store key types. The pages are populated from the head by pre-allocating a fixed number of slots.
 * Each slot is an integer as a pointer in the page of the page. Values start from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (values). Values are linked list and they contain 1 byte to store the compressed hash, a compressed_rid and an integer (4 bytes)
 * for the next item in list.
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 * <p>
 * HEADER 1st PAGE = [offsetFreeValueContent(int:4),entriesPerPage(int:4),numberOfKeys(byte:1),[keyType(byte:1)]*,[propertyNameIdx(int:4)]*]
 * <p>
 * HEADER Nst PAGE = [offsetFreeValueContent(int:4)]
 */
public class IndexLSMHash extends IndexLSMAbstract {
  public static final String UNIQUE_INDEX_EXT    = "uhidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nuhidx";

  private final String[] propertyNames;
  private final int      entriesPerPage;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final String[] propertyNames, final byte[] keyTypes, final int pageSize) throws IOException {
      if (propertyNames == null || propertyNames.length == 0)
        throw new IllegalArgumentException("LSM-Hash index requires property names");

      return new IndexLSMHash(database, name, unique, filePath, mode, propertyNames, keyTypes, pageSize);
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

  private class Hash {
    public final long hashedKey;
    public final long compressedHashedKey;
    public final long entryIndexPosition;

    public Hash(final Object[] keys, final int pointers) {
      hashedKey = BinaryTypes.getHash64(keys);
      final long absKey = Math.abs(hashedKey);
      compressedHashedKey = absKey % 128;
      entryIndexPosition = absKey % pointers;
    }
  }

  protected static class LookupResult {
    public final int       lastEntryNextPosition;
    public final List<RID> rids;

    public LookupResult(final List<RID> rids, final int lastEntryNextPosition) {
      this.lastEntryNextPosition = lastEntryNextPosition;
      this.rids = rids;
    }
  }

  /**
   * Called at creation time.
   */
  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode,
      final String[] propertyNames, final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, mode, keyTypes, pageSize);
    this.propertyNames = propertyNames;
    this.entriesPerPage = computeEntriesPerPage(pageSize); // SETTING ENTRIES TO 1/8 OF THE PAGE SIZE
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at cloning time.
   */
  public IndexLSMHash(final Database database, final String name, final boolean unique, String filePath, final String[] propertyNames, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize);
    this.propertyNames = propertyNames;
    this.entriesPerPage = computeEntriesPerPage(pageSize);
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

    int pos = INT_SERIALIZED_SIZE;

    entriesPerPage = currentPage.readInt(pos);
    pos += INT_SERIALIZED_SIZE;

    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);

    this.propertyNames = new String[len];
    for (int i = 0; i < len; ++i) {
      final int propIdx = currentPage.readInt(pos);
      this.propertyNames[i] = database.getSchema().getDictionary().getNameById(propIdx);
      pos += INT_SERIALIZED_SIZE;
    }
  }

  public IndexLSMHash copy() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
    return new IndexLSMHash(database, newName, unique, database.getDatabasePath() + "/" + newName, propertyNames, keyTypes, pageSize);
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
  public Set<RID> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    try {
      final Set<RID> set = new HashSet<>();

      final int totalPages = getTotalPages();

      final Set<RID> removedRIDs = new HashSet<>();

      final Hash hash = new Hash(keys, entriesPerPage);

      // SEARCH FROM THE LAST PAGE BACK
      for (int p = totalPages - 1; p > -1; --p) {
        final BasePage currentPage = this.database.getTransaction().getPage(new PageId(file.getFileId(), p), pageSize);
        final Binary currentPageBuffer = new Binary(currentPage.slice());

        final LookupResult result = lookupInPage(currentPage, currentPageBuffer, keys, hash);
        if (!result.rids.isEmpty()) {
          // START FROM THE LAST ENTRY
          for (int i = result.rids.size() - 1; i > -1; --i) {
            final RID rid = result.rids.get(i);

            if (rid.getBucketId() < 0) {
              // RID DELETED, SKIP THE RID
              final RID originalRID = getOriginalRID(rid);
              if (!set.contains(originalRID)) {
                removedRIDs.add(originalRID);
              }
              continue;
            }

            if (removedRIDs.contains(rid))
              // ALREADY FOUND AS DELETED
              continue;

            if (recordMatches(rid, keys)) {
              set.add(rid);
              if (limit > -1 && set.size() >= limit) {
                return set;
              }
            }
          }
        }
      }

      return set;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  @Override
  public void remove(final Object[] keys) {
    throw new UnsupportedOperationException("LSM-Hash index does not allow removal by key only");
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
   * Look ups for an entry in the index. The result must be filtered because for the nature of the hashing, it may contain non related entries in the same
   * container because conflicted keys.
   */
  protected LookupResult lookupInPage(final BasePage currentPage, final Binary currentPageBuffer, final Object[] keys, final Hash hash) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (keys.length > keyTypes.length)
      throw new IllegalArgumentException("key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if (keys.length < keyTypes.length)
      throw new IllegalArgumentException("Partial key matching not supported by LSM-Hash index");

    final int startIndexArray = getHeaderSize(currentPage.getPageId().getPageNumber());

    int pos = currentPageBuffer.getInt((int) (startIndexArray + (hash.entryIndexPosition * INT_SERIALIZED_SIZE)));
    if (pos > 0) {
      // FOUND
      final List<RID> result = new ArrayList<>();

      int lastEntryNextPos = -1;
      while (pos > 0) {
        currentPageBuffer.position(pos);
        final byte compressedHash = (byte) serializer.deserializeValue(database, currentPageBuffer, BinaryTypes.TYPE_BYTE);
        final RID rid = (RID) serializer.deserializeValue(database, currentPageBuffer, BinaryTypes.TYPE_COMPRESSED_RID);
        if (compressedHash == hash.compressedHashedKey)
          // PRE-FILTERING BY COMPRESSED HASH KEY
          result.add(rid);

        lastEntryNextPos = currentPageBuffer.position();
        pos = currentPageBuffer.getInt();
      }

      return new LookupResult(result, lastEntryNextPos);
    }

    // NOT FOUND
    return new LookupResult(Collections.EMPTY_LIST, -1);
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

    insertEntry(keys, rid, null);
  }

  @Override
  protected void internalRemove(final Object[] keys, final RID rid) {
    if (keys.length != keyTypes.length)
      throw new IllegalArgumentException("Cannot remove an entry in the index with a partial key");

    final RID removedRID = getRemovedRID(rid);

    insertEntry(keys, rid, new Callable<RID, LookupResult>() {
      @Override
      public RID call(final LookupResult result) {
        for (int i = result.rids.size() - 1; i > -1; --i) {
          final RID entryRID = result.rids.get(i);
          if (entryRID.equals(rid))
            // FOUND
            break;
          if (removedRID.equals(entryRID))
            // ALREADY DELETED
            return null;
        }
        return removedRID;
      }
    });
  }

  private void insertEntry(final Object[] keys, final RID rid, final Callable<RID, LookupResult> callback) {
    checkForNulls(keys);

    database.checkTransactionIsActive();

    final int txPageCounter = getTotalPages();

    int pageNum = txPageCounter - 1;

    try {
      ModifiablePage currentPage = database.getTransaction().getPageToModify(new PageId(file.getFileId(), pageNum), pageSize, false);

      TrackableBinary currentPageBuffer = currentPage.getTrackable();

      final Hash hash = new Hash(keys, entriesPerPage);

      final LookupResult result = lookupInPage(currentPage, currentPageBuffer, keys, hash);

      RID ridToInsert = rid;
      if (callback != null) {
        ridToInsert = callback.call(result);
        if (ridToInsert == null)
          return;
      }

      // WRITE VALUE FIRST
      final Binary valueContent = database.getContext().getTemporaryBuffer1();
      valueContent.putByte((byte) hash.compressedHashedKey); // COMPRESSED HASH
      valueContent.putNumber(ridToInsert.getBucketId()); // COMPRESSED RID
      valueContent.putNumber(ridToInsert.getPosition()); // COMPRESSED RID
      valueContent.putInt(-1); // NEXT

      int valuesFreePosition = getValuesFreePosition(currentPage);
      int entryIndex = (int) hash.entryIndexPosition;
      int pos = -1;

      boolean newPage = false;
      if (valuesFreePosition - (getHeaderSize(pageNum) + (entriesPerPage * INT_SERIALIZED_SIZE)) < valueContent.size()) {
        // NO SPACE LEFT, CREATE A NEW PAGE
        newPage = true;

        //countEntriesInPage(currentPage, currentPageBuffer);

        currentPage = createNewPage();
        currentPageBuffer = currentPage.getTrackable();
        pageNum = currentPage.getPageId().getPageNumber();
        valuesFreePosition = currentPage.getMaxContentSize();
      } else {
        if (result.lastEntryNextPosition > -1)
          // WRITE IN THE NEXT SPACE OF LAST ENTRY OF THE LINKED LIST
          pos = result.lastEntryNextPosition;
      }

      valuesFreePosition -= valueContent.size();

      // WRITE KEY/VALUE PAIR CONTENT
      currentPageBuffer.putByteArray(valuesFreePosition, valueContent.toByteArray());

      if (pos == -1)
        pos = getHeaderSize(pageNum) + (entryIndex * INT_SERIALIZED_SIZE);

      currentPageBuffer.putInt(pos, valuesFreePosition);

      setValuesFreePosition(currentPage, valuesFreePosition);

      LogManager.instance()
          .debug(this, "Put entry %s=%s in index '%s' (page=%s newPage=%s)", Arrays.toString(keys), ridToInsert, name, currentPage.getPageId(), newPage);

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  // TEST ONLY
  private int countEntriesInPage(final BasePage currentPage, final Binary currentPageBuffer) {
    final int startIndexArray = getHeaderSize(currentPage.getPageId().getPageNumber());

    int total = 0;
    int usedBuckets = 0;
    int freeBuckets = 0;

    for (int i = 0; i < entriesPerPage; ++i) {
      int pos = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE));
      if (pos > 0) {
        ++usedBuckets;
        while (pos > 0) {
          currentPageBuffer.position(pos);
          serializer.deserializeValue(database, currentPageBuffer, BinaryTypes.TYPE_BYTE);
          serializer.deserializeValue(database, currentPageBuffer, BinaryTypes.TYPE_COMPRESSED_RID);
          pos = currentPageBuffer.getInt();

          total++;
        }
      } else
        ++freeBuckets;
    }

    LogManager.instance()
        .info(this, "HASH INDEX STATS: entries=%d cfgEntriesPerPage=%d usedBuckets=%d freeBuckets=%d avgEntriesPerKey=%d", total, entriesPerPage, usedBuckets,
            freeBuckets, total / usedBuckets);

    return total;
  }

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE;
    if (pageNum == 0)
      size += INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + keyTypes.length + (keyTypes.length * INT_SERIALIZED_SIZE);
    return size;
  }

  private ModifiablePage createNewPage() {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final ModifiablePage currentPage = database.getTransaction().addPage(new PageId(file.getFileId(), txPageCounter), pageSize);

    int pos = 0;

    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    if (txPageCounter == 0) {
      currentPage.writeInt(pos, entriesPerPage);
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i)
        currentPage.writeByte(pos++, keyTypes[i]);
      for (int i = 0; i < keyTypes.length; ++i) {
        currentPage.writeInt(pos, database.getSchema().getDictionary().getIdByName(propertyNames[i], false));
        pos += INT_SERIALIZED_SIZE;
      }
    }

    return currentPage;
  }

  private boolean recordMatches(final RID rid, final Object[] keys) {
    try {
      final Document record = (Document) database.lookupByRID(rid, false);

      boolean matches = true;
      for (int k = 0; matches && k < keys.length; ++k) {
        final Object propertyValue = record.get(propertyNames[k]);
        if (propertyValue == null || !propertyValue.equals(keys[k]))
          matches = false;
      }
      return matches;

    } catch (RecordNotFoundException e) {
      return false;
    }
  }

  private int computeEntriesPerPage(final int pageSize) {
    return pageSize / 15 / 4;
  }
}
