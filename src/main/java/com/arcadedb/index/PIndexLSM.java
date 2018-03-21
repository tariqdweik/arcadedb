package com.arcadedb.index;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.*;
import com.arcadedb.exception.PConfigurationException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.serializer.PBinaryComparator;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.serializer.PBinaryTypes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.database.PBinary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.PBinary.INT_SERIALIZED_SIZE;

/**
 * LSM-Tree index. The first page contains 2 bytes to store key and value types. The pages are populated from the head of the page
 * with the pointers to the pair key/value that starts from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (key/value pairs).
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 * <p>
 * HEADER 1st PAGE = [numberOfEntries(int:4),offsetFreeKeyValueContent(int:4),bloomFilterSeed(int:4),
 * bloomFilter(bytes[]:<bloomFilterLength>), numberOfKeys(byte:1),keyType(byte:1)*,valueType(byte:1)]
 * <p>
 * HEADER Nst PAGE = [numberOfEntries(int:4),offsetFreeKeyValueContent(int:4),bloomFilterSeed(int:4),
 * bloomFilter(bytes[]:<bloomFilterLength>)]
 */
public class PIndexLSM extends PPaginatedFile implements PIndex {
  public static final String INDEX_EXT     = "pindex";
  public static final int    DEF_PAGE_SIZE = 6553600;

  private byte[] keyTypes;
  private byte   valueType;
  private volatile boolean compacting = false;

  private AtomicLong statsBFFalsePositive = new AtomicLong();
  private AtomicLong statsAdjacentSteps   = new AtomicLong();

  protected class LookupResult {
    public final boolean found;
    public final int     keyIndex;
    public final int     valueBeginPosition;

    public LookupResult(final boolean found, final int keyIndex, final int valueBeginPosition) {
      this.found = found;
      this.keyIndex = keyIndex;
      this.valueBeginPosition = valueBeginPosition;
    }
  }

  /**
   * Called at creation time.
   */
  public PIndexLSM(final PDatabase database, final String name, String filePath, final PFile.MODE mode, final byte[] keyTypes,
      final byte valueType, final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), PIndexLSM.INDEX_EXT, mode, pageSize);
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at cloning time.
   */
  public PIndexLSM(final PDatabase database, final String name, String filePath, final byte[] keyTypes, final byte valueType,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), "temp_" + PIndexLSM.INDEX_EXT, PFile.MODE.READ_WRITE,
        pageSize);
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    database.checkTransactionIsActive();
    createNewPage();
  }

  /**
   * Called at load time (1st page only).
   */
  public PIndexLSM(final PDatabase database, final String name, String filePath, final int id, final PFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    final PBasePage currentPage = this.database.getTransaction().getPage(new PPageId(file.getFileId(), 0), pageSize);

    int pos = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    final int len = currentPage.readByte(pos++);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(pos++);
    this.valueType = currentPage.readByte(pos++);
  }

  public PIndexLSM copy() throws IOException {
    int last_ = name.lastIndexOf('_');
    final String newName = name.substring(0, last_) + "_" + System.currentTimeMillis();
    return new PIndexLSM(database, newName, database.getDatabasePath() + "/" + newName, keyTypes, valueType, pageSize);
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
      final PIndexLSMCompactor compactor = new PIndexLSMCompactor(this);
      compactor.compact();
    } finally {
      compacting = false;
    }
  }

  @Override
  public PIndexCursor iterator(final boolean ascendingOrder) throws IOException {
    return new PIndexLSMCursor(this, ascendingOrder);
  }

  @Override
  public PIndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) throws IOException {
    if (ascendingOrder)
      return range(fromKeys, null);

    return range(null, fromKeys);
  }

  @Override
  public PIndexCursor iterator(final Object[] fromKeys) throws IOException {
    return range(fromKeys, fromKeys);
  }

  @Override
  public PIndexCursor range(final Object[] fromKeys, final Object[] toKeys) throws IOException {
    return new PIndexLSMCursor(this, true, fromKeys, toKeys);
  }

  public PIndexLSMPageIterator newPageIterator(final int pageId, final int currentEntryInPage, final boolean ascendingOrder)
      throws IOException {
    final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), pageSize);
    return new PIndexLSMPageIterator(this, page, currentEntryInPage, getHeaderSize(pageId), keyTypes, getCount(page), ascendingOrder);
  }

  @Override
  public List<PRID> get(final Object[] keys) {
    try {
      final List<PRID> list = new ArrayList<>();

      final int totalPages = getTotalPages();

      for (int p = 0; p < totalPages; ++p) {
        final PBasePage currentPage = this.database.getTransaction().getPage(new PPageId(file.getFileId(), p), pageSize);
        final PBinary currentPageBuffer = new PBinary(currentPage.slice());
        final int count = getCount(currentPage);

        // SEARCH IN THE BF FIRST
        final LookupResult result = searchInPage(currentPage, currentPageBuffer, keys, count, 0);
        if (result != null && result.found)
          list.add((PRID) getValue(currentPageBuffer, database.getSerializer(), result.valueBeginPosition));
      }

      return list;

    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  @Override
  public void put(final Object[] keys, final PRID rid) {
    if (rid == null)
      throw new IllegalArgumentException("RID is null");

    database.checkTransactionIsActive();

    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount;

    int pageNum = txPageCounter - 1;

    try {
      PModifiablePage currentPage = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), pageNum), pageSize);

      PBinary currentPageBuffer = new PBinary(currentPage.slice());

      int count = getCount(currentPage);

      final LookupResult result = lookupInPage(pageNum, count, currentPageBuffer, keys, 0);
      if (result.found)
        // TODO: MANAGE ALL THE CASES
        return;
      //throw new PDuplicatedKeyException("Key '" + key + "' already exists");

      // WRITE KEY/VALUE PAIRS FIRST
      final PBinary keyValueContent = new PBinary();
      // MULTI KEYS
      for (int i = 0; i < keyTypes.length; ++i)
        database.getSerializer().serializeValue(keyValueContent, keyTypes[i], keys[i]);

      database.getSerializer().serializeValue(keyValueContent, valueType, rid);

      int keyValueFreePosition = getKeyValueFreePosition(currentPage);

      int keyIndex = result.keyIndex;
      if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent
          .size()) {
        // NO SPACE LEFT, CREATE A NEW PAGE
        try {
          database.getTransaction().addPageToDispose(currentPage.getPageId());
          currentPage = createNewPage();
          currentPageBuffer = new PBinary(currentPage.slice());
          pageNum = currentPage.getPageId().getPageNumber();
          count = 0;
          keyIndex = 0;
          keyValueFreePosition = currentPage.getMaxContentSize();

        } catch (IOException e) {
          throw new PConfigurationException("Cannot create a new index page", e);
        }
      }

      keyValueFreePosition -= keyValueContent.size();

      // WRITE KEY/VALUE PAIR CONTENT
      currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

      // SHIFT POINTERS ON THE RIGHT
      final int startPos = getHeaderSize(pageNum) + (keyIndex * INT_SERIALIZED_SIZE);
      currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

      currentPageBuffer.putInt(startPos, keyValueFreePosition);

      // ADD THE ITEM IN THE BF
      final BufferBloomFilter bf = new BufferBloomFilter(
          currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
          getBFSeed(currentPage));

      // COMPUTE BF FOR ALL THE COMBINATIONS OF THE KEYS
      for (int k = 1; k <= keys.length; ++k)
        bf.add(PBinaryTypes.getHash(keys, k));

      setCount(currentPage, count + 1);
      setKeyValueFreePosition(currentPage, keyValueFreePosition);

    } catch (IOException e) {
      throw new PDatabaseOperationException(
          "Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  public PModifiablePage appendDuringCompaction(final PBinary keyValueContent, PModifiablePage currentPage,
      PBinary currentPageBuffer, final Object[] keys, final PRID rid) {
    if (currentPage == null) {

      Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
      if (txPageCounter == null)
        txPageCounter = pageCount;

      try {
        currentPage = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), txPageCounter - 1), pageSize);
        currentPageBuffer = new PBinary(currentPage.slice());
      } catch (IOException e) {
        throw new PDatabaseOperationException(
            "Cannot append key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
      }
    }

    int count = getCount(currentPage);

    int pageNum = currentPage.getPageId().getPageNumber();

    keyValueContent.reset();

    // MULTI KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      database.getSerializer().serializeValue(keyValueContent, keyTypes[i], keys[i]);

    database.getSerializer().serializeValue(keyValueContent, valueType, rid);

    int keyValueFreePosition = getKeyValueFreePosition(currentPage);

    if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent
        .size()) {
      // NO SPACE LEFT, CREATE A NEW PAGE
      try {
        database.getTransaction().addPageToDispose(currentPage.getPageId());
        database.getTransaction().commit();
        database.getTransaction().begin();
        currentPage = createNewPage();
        currentPageBuffer = new PBinary(currentPage.slice());
        pageNum = currentPage.getPageId().getPageNumber();
        count = 0;
        keyValueFreePosition = currentPage.getMaxContentSize();
      } catch (IOException e) {
        throw new PConfigurationException("Cannot create a new index page", e);
      }
    }

    keyValueFreePosition -= keyValueContent.size();

    // WRITE KEY/VALUE PAIR CONTENT
    currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

    final int startPos = getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE);
    currentPageBuffer.putInt(startPos, keyValueFreePosition);

    // ADD THE ITEM IN THE BF
    final BufferBloomFilter bf = new BufferBloomFilter(
        currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(),
        getBFSeed(currentPage));

    for (int k = 1; k <= keys.length; ++k)
      bf.add(PBinaryTypes.getHash(keys, k));

    setCount(currentPage, count + 1);
    setKeyValueFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
  }

  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
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

  protected LookupResult searchInPage(final PBasePage currentPage, final PBinary currentPageBuffer, final Object[] keys,
      final int count, final int purpose) {
    // SEARCH IN THE BF FIRST
    final int seed = getBFSeed(currentPage);

    final BufferBloomFilter bf = new BufferBloomFilter(
        currentPageBuffer.slice(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE), getBFSize(), seed);

    if (bf.mightContain(PBinaryTypes.getHash(keys)))
      return lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, keys, purpose);
    else
      statsBFFalsePositive.incrementAndGet();

    return null;
  }

  protected int getTotalPages() {
    final Integer txPageCounter = database.getTransaction().getPageCounter(id);
    if (txPageCounter != null)
      return txPageCounter;
    try {
      return (int) (file.getSize() / pageSize);
    } catch (IOException e) {
      throw new PIndexException("Error on determine the total pages", e);
    }
  }

  /**
   * Lookup for an entry in the index.
   *
   * @param pageNum
   * @param count
   * @param currentPageBuffer
   * @param keys
   * @param purpose           0 = exists, 1 = ascending iterator, 2 = descending iterator
   *
   * @return
   */
  protected LookupResult lookupInPage(final int pageNum, final int count, final PBinary currentPageBuffer, final Object[] keys,
      final int purpose) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (keys.length > keyTypes.length)
      throw new IllegalArgumentException(
          "key is composed of " + keys.length + " items, while the index defined " + keyTypes.length + " items");

    if (count == 0)
      // EMPTY NOT FOUND
      return new LookupResult(false, 0, -1);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize(pageNum);

    final PBinarySerializer serializer = database.getSerializer();
    final PBinaryComparator comparator = serializer.getComparator();

    while (low <= high) {
      int mid = (low + high) / 2;

      final int contentPos = currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE));
      currentPageBuffer.position(contentPos);

      int result;
      boolean found = false;
      for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
        // GET THE KEY

        if (keyTypes[keyIndex] == PBinaryTypes.TYPE_STRING) {
          // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
          result = comparator.compareStrings((String) keys[keyIndex], currentPageBuffer);
        } else {
          final Object key = serializer.deserializeValue(database, currentPageBuffer, keyTypes[keyIndex]);
          result = comparator.compare(keys[keyIndex], keyTypes[keyIndex], key, keyTypes[keyIndex]);
        }

        if (result > 0) {
          low = mid + 1;
          found = false;
          break;
        } else if (result < 0) {
          high = mid - 1;
          found = false;
          break;
        } else {
          // FOUND CONTINUE WITH THE NEXT KEY IN THE ARRAY
          found = true;
        }
      }

      if (found && keys.length < keyTypes.length) {
        // PARTIAL MATCHING
        if (purpose == 1) {
          // FIND THE MOST LEFT ITEM
          int jump = 5;
          for (int i = mid - 1; i >= 0; i -= jump) {
            final int pos = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE));
            currentPageBuffer.position(pos);

            result = 1;
            for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
              if (keyTypes[keyIndex] == PBinaryTypes.TYPE_STRING) {
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

              if (i > 0 && i - jump < 0)
                jump = i;
            } else {
              if (jump == 1)
                break;

              i += jump;
              jump /= 2;
              if (jump < 1)
                jump = 1;
            }
          }
        } else if (purpose == 2) {
          // FIND THE MOST RIGHT ITEM
          for (int i = mid + 1; i < count; ++i) {
            final int pos = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE));
            currentPageBuffer.position(pos);

            result = 1;
            for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
              if (keyTypes[i] == PBinaryTypes.TYPE_STRING) {
                // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
                result = comparator.compareStrings((String) keys[i], currentPageBuffer);
              } else {
                final Object key = serializer.deserializeValue(database, currentPageBuffer, keyTypes[i]);
                result = comparator.compare(keys[i], keyTypes[i], key, keyTypes[i]);
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

        }
      }

      if (found)
        return new LookupResult(true, mid, currentPageBuffer.position());
    }

    // NOT FOUND
    return new LookupResult(false, low, -1);
  }

  protected Object getValue(final PBinary currentPageBuffer, final PBinarySerializer serializer, final int valueBeginPosition) {
    currentPageBuffer.position(valueBeginPosition);
    return serializer.deserializeValue(database, currentPageBuffer, valueType);
  }

  private int getHeaderSize(final int pageNum) {
    int size = INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + getBFSize();
    if (pageNum == 0)
      size += BYTE_SERIALIZED_SIZE + keyTypes.length + BYTE_SERIALIZED_SIZE;
    return size;
  }

  private PModifiablePage createNewPage() throws IOException {
    // NEW FILE, CREATE HEADER PAGE
    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount;

    final PModifiablePage currentPage = database.getTransaction().addPage(new PPageId(file.getFileId(), txPageCounter), pageSize);

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

    if (pageCount == 0) {
      currentPage.writeByte(pos++, (byte) keyTypes.length);
      for (int i = 0; i < keyTypes.length; ++i) {
        currentPage.writeByte(pos++, keyTypes[i]);
      }
      currentPage.writeByte(pos++, valueType);
    }

    return currentPage;
  }

  protected static int compareKeys(final PBinaryComparator comparator, final byte[] keyTypes, final Object[] keys1,
      final Object[] keys2) {
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

  protected int getCount(final PBasePage currentPage) {
    return currentPage.readInt(0);
  }

  private void setCount(final PModifiablePage currentPage, final int newCount) {
    currentPage.writeInt(0, newCount);
  }

  private int getKeyValueFreePosition(final PBasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE);
  }

  private void setKeyValueFreePosition(final PModifiablePage currentPage, final int newKeyValueFreePosition) {
    currentPage.writeInt(INT_SERIALIZED_SIZE, newKeyValueFreePosition);
  }

  protected int getBFSeed(final PBasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  private int getBFSize() {
    return pageSize / 15 / 8 * 8;
  }
}