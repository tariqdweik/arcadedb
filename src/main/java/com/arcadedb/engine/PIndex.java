package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PRID;
import com.arcadedb.exception.PConfigurationException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.serializer.PBinaryComparator;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.serializer.PBinaryTypes;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;

import static com.arcadedb.database.PBinary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.PBinary.INT_SERIALIZED_SIZE;

/**
 * LSM-Tree index. The first page contains 2 byte to store key and value types. The pages are populated from the head of the page
 * with the pointers to the pair key/value that starts from the tail. A page is full when there is no space anymore between the head
 * (key pointers) and the tail (key/value pairs).
 * <p>
 * When a page is full, another page is created, waiting for a compaction.
 */
public class PIndex extends PPaginatedFile {
  public static final String INDEX_EXT = "pindex";
  public static final int    PAGE_SIZE = 655360;

  private byte[] keyTypes;
  private byte   valueType;

  private class LookupResult {
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
  public PIndex(final PDatabase database, final String name, String filePath, final PFile.MODE mode, final byte[] keyTypes,
      final byte valueType) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), PIndex.INDEX_EXT, mode);
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    database.checkTransactionIsActive();
    PModifiablePage page = createNewPage();
    page.writeByte(BYTE_SERIALIZED_SIZE, (byte) 0); // VALUES NOT ORDERED BY DEFAULT
  }

  /**
   * Called at compaction time.
   */
  public PIndex(final PDatabase database, final String name, String filePath, final int id, final PFile.MODE mode,
      final byte[] keyTypes, final byte valueType) throws IOException {
    super(database, name, filePath, id, PIndex.INDEX_EXT, mode);
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    database.checkTransactionIsActive();
    PModifiablePage page = createNewPage();
    page.writeByte(BYTE_SERIALIZED_SIZE, (byte) 1); // DURING COMPACTION, VALUES ARE ORDERED
  }

  /**
   * Called at load time.
   */
  public PIndex(final PDatabase database, final String name, String filePath, final int id, final PFile.MODE mode)
      throws IOException {
    super(database, name, filePath, id, mode);
    final PBasePage currentPage = this.database.getTransaction().getPage(new PPageId(file.getFileId(), pageCount - 1), PAGE_SIZE);

    int pos = BYTE_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;
    final int len = currentPage.readByte(pos);
    this.keyTypes = new byte[len];
    for (int i = 0; i < len; ++i)
      this.keyTypes[i] = currentPage.readByte(++pos);
    this.valueType = currentPage.readByte(++pos);
  }

  public PIndex copy() throws IOException {
    final String newName = name + "-temp";
    return new PIndex(database, newName, database.getDatabasePath() + "/" + newName, PFile.MODE.READ_WRITE, keyTypes, valueType);
  }

  public PIndexIterator newIterator(final int pageId) throws IOException {
    final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), PIndex.PAGE_SIZE);
    return new PIndexIterator(this, new PBinary(page.slice()), getHeaderSize(), keyTypes, getCount(page));
  }

  public PRID get(final Object[] keys) {
    database.checkTransactionIsActive();
    try {
      final PBasePage currentPage = this.database.getTransaction().getPage(new PPageId(file.getFileId(), pageCount - 1), PAGE_SIZE);
      final PBinary currentPageBuffer = new PBinary(currentPage.slice());

      int count = getCount(currentPage);

      final LookupResult result = lookup(count, currentPageBuffer, keys);
      if (result.found)
        return (PRID) getValue(currentPageBuffer, database.getSerializer(), result.valueBeginPosition);

      return null;

    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  public void put(final Object[] keys, final PRID rid) {
    database.checkTransactionIsActive();

    Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
    if (txPageCounter == null)
      txPageCounter = pageCount;

    try {
      PModifiablePage currentPage = database.getTransaction()
          .getPageToModify(new PPageId(file.getFileId(), txPageCounter - 1), PAGE_SIZE);

      PBinary currentPageBuffer = new PBinary(currentPage.slice());

      int count = getCount(currentPage);

      final LookupResult result = lookup(count, currentPageBuffer, keys);
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
      if (keyValueFreePosition - (getHeaderSize() + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
        // NO SPACE LEFT, CREATE A NEW PAGE
        try {
//          checkPage(currentPage, currentPageBuffer);
          database.getTransaction().addPageToDispose(currentPage.pageId);
          currentPage = createNewPage();
          currentPageBuffer = new PBinary(currentPage.slice());
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
      final int startPos = getHeaderSize() + (keyIndex * INT_SERIALIZED_SIZE);
      currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE, (count - keyIndex) * INT_SERIALIZED_SIZE);

      currentPageBuffer.putInt(startPos, keyValueFreePosition);

      setCount(currentPage, count + 1);
      setKeyValueFreePosition(currentPage, keyValueFreePosition);

    } catch (IOException e) {
      throw new PDatabaseOperationException(
          "Cannot index key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
    }
  }

  public PModifiablePage appendDuringCompaction(PModifiablePage currentPage, PBinary currentPageBuffer, final Object[] keys,
      final PRID rid) {
    if (currentPage == null) {

      Integer txPageCounter = database.getTransaction().getPageCounter(file.getFileId());
      if (txPageCounter == null)
        txPageCounter = pageCount;

      try {
        currentPage = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), txPageCounter - 1), PAGE_SIZE);
        currentPageBuffer = new PBinary(currentPage.slice());
      } catch (IOException e) {
        throw new PDatabaseOperationException(
            "Cannot append key '" + Arrays.toString(keys) + "' with value '" + rid + "' in index '" + name + "'", e);
      }
    }

    int count = getCount(currentPage);

    // WRITE KEY/VALUE PAIRS FIRST
    final PBinary keyValueContent = new PBinary();
    // MULTI KEYS
    for (int i = 0; i < keyTypes.length; ++i)
      database.getSerializer().serializeValue(keyValueContent, keyTypes[i], keys[i]);

    database.getSerializer().serializeValue(keyValueContent, valueType, rid);

    int keyValueFreePosition = getKeyValueFreePosition(currentPage);

    if (keyValueFreePosition - (getHeaderSize() + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
      // NO SPACE LEFT, CREATE A NEW PAGE
      try {
        database.getTransaction().addPageToDispose(currentPage.pageId);
        database.getTransaction().commit();
        database.getTransaction().begin();
        currentPage = createNewPage();
        currentPageBuffer = new PBinary(currentPage.slice());
        count = 0;
        keyValueFreePosition = currentPage.getMaxContentSize();
      } catch (IOException e) {
        throw new PConfigurationException("Cannot create a new index page", e);
      }
    }

    keyValueFreePosition -= keyValueContent.size();

    // WRITE KEY/VALUE PAIR CONTENT
    currentPageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

    final int startPos = getHeaderSize() + (count * INT_SERIALIZED_SIZE);
    currentPageBuffer.putInt(startPos, keyValueFreePosition);

    setCount(currentPage, count + 1);
    setKeyValueFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
  }

  @Override
  public String toString() {
    return name;
  }

  public byte[] getKeyTypes() {
    return keyTypes;
  }

  protected int getTotalPages() throws IOException {
    return (int) (file.getSize() / PAGE_SIZE);
  }

  @Override
  protected long getPageSize() {
    return PAGE_SIZE;
  }

  private LookupResult lookup(final int count, final PBinary currentPageBuffer, final Object[] keys) {
    if (keyTypes.length == 0)
      throw new IllegalArgumentException("No key types found");

    if (count == 0)
      // EMPTY NOT FOUND
      return new LookupResult(false, 0, -1);

    int low = 0;
    int high = count - 1;

    final int startIndexArray = getHeaderSize();

    final PBinarySerializer serializer = database.getSerializer();
    final PBinaryComparator comparator = serializer.getComparator();

    while (low <= high) {
      int mid = (low + high) / 2;

      final int contentPos = currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE));
      currentPageBuffer.position(contentPos);

      int result;
      boolean found = false;
      for (int i = 0; i < keyTypes.length; ++i) {
        // GET THE KEY

        if (keyTypes[i] == PBinaryTypes.TYPE_STRING) {
          // OPTIMIZATION: SPECIAL CASE, LAZY EVALUATE BYTE PER BYTE THE STRING
          result = comparator.compareStrings((String) keys[i], currentPageBuffer);
        } else {
          final Object key = serializer.deserializeValue(currentPageBuffer, keyTypes[i]);
          result = comparator.compare(keys[i], keyTypes[i], key, keyTypes[i]);
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

      if (found)
        return new LookupResult(true, mid, currentPageBuffer.position());
    }

    // NOT FOUND
    return new LookupResult(false, low, -1);
  }

  protected Object getValue(final PBinary currentPageBuffer, final PBinarySerializer serializer, final int valueBeginPosition) {
    currentPageBuffer.position(valueBeginPosition);
    return serializer.deserializeValue(currentPageBuffer, valueType);
  }

  private int getHeaderSize() {
    return BYTE_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE + keyTypes.length
        + BYTE_SERIALIZED_SIZE;
  }

  private PModifiablePage createNewPage() throws IOException {
    // NEW FILE, CREATE HEADER PAGE
    final boolean txActive = database.isTransactionActive();
    if (!txActive)
      this.database.begin();

    final PModifiablePage currentPage = database.getTransaction().addPage(new PPageId(file.getFileId(), pageCount), PAGE_SIZE);

    int pos = 0;
    currentPage.writeByte(pos, (byte) 0);
    currentPage.writeInt(++pos, 0);
    currentPage.writeInt(pos += INT_SERIALIZED_SIZE, currentPage.getMaxContentSize());
    currentPage.writeByte(pos += INT_SERIALIZED_SIZE, (byte) keyTypes.length);
    for (int i = 0; i < keyTypes.length; ++i)
      currentPage.writeByte(++pos, keyTypes[i]);
    currentPage.writeByte(++pos, valueType);

    if (!txActive)
      this.database.commit();

    ++pageCount;

    return currentPage;
  }

  private boolean isOrdered(final PBasePage currentPage) {
    return currentPage.readByte(0) == 1;
  }

  private int getCount(final PBasePage currentPage) {
    return currentPage.readInt(BYTE_SERIALIZED_SIZE);
  }

  private void setCount(final PModifiablePage currentPage, final int newCount) {
    currentPage.writeInt(BYTE_SERIALIZED_SIZE, newCount);
  }

  private int getKeyValueFreePosition(final PModifiablePage currentPage) {
    return currentPage.readInt(BYTE_SERIALIZED_SIZE + INT_SERIALIZED_SIZE);
  }

  private void setKeyValueFreePosition(final PModifiablePage currentPage, final int newKeyValueFreePosition) {
    currentPage.writeInt(BYTE_SERIALIZED_SIZE + INT_SERIALIZED_SIZE, newKeyValueFreePosition);
  }

  private void checkPage(final PModifiablePage page, final PBinary currentPageBuffer) {
    final int total = getCount(page);

    final int startIndexArray = getHeaderSize();

    final PBinarySerializer serializer = database.getSerializer();
    final PBinaryComparator comparator = serializer.getComparator();

    final Object[] previousKeys = new Object[keyTypes.length];

    for (int i = 0; i < total; ++i) {

      final int contentPos = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE));

      for (int k = 0; k < keyTypes.length; ++k) {
        // GET THE KEY
        currentPageBuffer.position(contentPos);

        final Object key = serializer.deserializeValue(currentPageBuffer, keyTypes[k]);
        if (i > 0) {
          final int result = comparator.compare(key, keyTypes[k], previousKeys[k], keyTypes[k]);
          Assert.assertTrue(result > 0);
        }

        previousKeys[k] = key;
      }
    }
  }
}