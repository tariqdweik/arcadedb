package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.exception.PSchemaException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HEADER = [itemCount(int:4),pageSize(int:4)] CONTENT-PAGES = [propertyName(string)]
 * <p>
 */
public class PDictionary extends PPaginatedComponent {
  public static final String DICT_EXT      = "pdict";
  public static final int    DEF_PAGE_SIZE = 65536 * 5;

  private int itemCount;

  private final List<String>         dictionary    = new ArrayList<String>();
  private final Map<String, Integer> dictionaryMap = new HashMap<String, Integer>();

  private static final int DICTIONARY_ITEM_COUNT  = 0;
  private static final int DICTIONARY_HEADER_SIZE = PBinary.INT_SERIALIZED_SIZE;

  /**
   * Called at creation time.
   */
  public PDictionary(final PDatabase database, final String name, String filePath, final PPaginatedFile.MODE mode, final int pageSize)
      throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), DICT_EXT, mode, pageSize);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final PModifiablePage header = database.getTransaction().addPage(new PPageId(file.getFileId(), 0), pageSize);
      itemCount = 0;
      updateCounters(header);
    }
  }

  /**
   * Called at load time.
   */
  public PDictionary(final PDatabase database, final String name, String filePath, final int id, final PPaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final PModifiablePage header = database.getTransaction().addPage(new PPageId(file.getFileId(), 0), pageSize);
      itemCount = 0;
      updateCounters(header);

    } else {
      final PBasePage header = database.getTransaction().getPage(new PPageId(file.getFileId(), 0), pageSize);
      itemCount = header.readInt(DICTIONARY_ITEM_COUNT);

      // LOAD THE DICTIONARY IN RAM
      header.setBufferPosition(DICTIONARY_HEADER_SIZE);
      for (int i = 0; i < itemCount; ++i) {
        dictionary.add(new String(header.readString()));
      }

      for (int i = 0; i < dictionary.size(); ++i)
        dictionaryMap.put(dictionary.get(i), i);
    }

  }

  public int getIdByName(final String name, final boolean create) {
    if (name == null)
      throw new IllegalArgumentException("Dictionary item name was null");

    Integer pos = dictionaryMap.get(name);
    if (pos == null && create) {
      dictionary.add(name);
      pos = dictionaryMap.size();
      dictionaryMap.put(name, pos);
      addItem(name);
    }
    if (pos == null)
      return -1;

    return pos;
  }

  public String getNameById(final int nameId) {
    if (nameId < 0)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " is not valid");

    final String itemName = dictionary.get(nameId);
    if (nameId < 0)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " was not found");

    return itemName;
  }

  public void addItem(final String propertyName) {
    if (!database.isTransactionActive())
      throw new PSchemaException("Error on adding new idem to the database schema dictionary because no transaction was active");

    final byte[] property = propertyName.getBytes();

    final PModifiablePage header;
    try {
      header = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), 0), pageSize, false);

      if (header.getAvailableContentSize() < PBinary.SHORT_SERIALIZED_SIZE + property.length)
        throw new PDatabaseMetadataException("No space left in dictionary file (items=" + itemCount + ")");

      header.writeString(header.getContentSize(), propertyName);

      itemCount++;

      updateCounters(header);
    } catch (IOException e) {
      throw new PSchemaException("Error on adding new idem to the database schema dictionary");
    }
  }

  private void updateCounters(final PModifiablePage header) {
    header.writeInt(DICTIONARY_ITEM_COUNT, itemCount);
  }
}
