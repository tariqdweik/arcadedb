/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * HEADER = [itemCount(int:4),pageSize(int:4)] CONTENT-PAGES = [propertyName(string)]
 * <p>
 */
public class Dictionary extends PaginatedComponent {
  public static final String DICT_EXT      = "dict";
  public static final int    DEF_PAGE_SIZE = 65536 * 5;

  private int itemCount;

  private final List<String>         dictionary    = new ArrayList<String>();
  private final Map<String, Integer> dictionaryMap = new HashMap<String, Integer>();
  private final RWLockContext        lock          = new RWLockContext();

  private static final int DICTIONARY_ITEM_COUNT  = 0;
  private static final int DICTIONARY_HEADER_SIZE = Binary.INT_SERIALIZED_SIZE;

  public static class PaginatedComponentFactoryHandler implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(Database database, String name, String filePath, final int fileId, PaginatedFile.MODE mode, int pageSize)
        throws IOException {
      return new Dictionary(database, name, filePath, fileId, mode, pageSize);
    }
  }

  /**
   * Called at creation time.
   */
  public Dictionary(final Database database, final String name, String filePath, final PaginatedFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, DICT_EXT, mode, pageSize);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final MutablePage header = database.getTransaction().addPage(new PageId(file.getFileId(), 0), pageSize);
      itemCount = 0;
      updateCounters(header);
    }
  }

  /**
   * Called at load time.
   */
  public Dictionary(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode, final int pageSize)
      throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final MutablePage header = database.getTransaction().addPage(new PageId(file.getFileId(), 0), pageSize);
      itemCount = 0;
      updateCounters(header);

    } else {
      final BasePage header = database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);
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
      pos = (Integer) lock.executeInWriteLock(new Callable<Object>() {
        @Override
        public Object call() {
          Integer pos = dictionaryMap.get(name);
          if (pos == null) {
            addItemToPage(name);

            dictionary.add(name);
            dictionaryMap.put(name, itemCount - 1);

            return itemCount - 1;
          }
          return pos;
        }
      });
    }

    if (pos == null)
      return -1;

    return pos;
  }

  public String getNameById(final int nameId) {
    if (nameId < 0 || nameId >= dictionary.size())
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " is not valid");

    final String itemName = dictionary.get(nameId);
    if (itemName == null)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " was not found");

    return itemName;
  }

  private void addItemToPage(final String propertyName) {
    if (!database.isTransactionActive())
      throw new SchemaException("Error on adding new item to the database schema dictionary because no transaction was active");

    final byte[] property = propertyName.getBytes();

    final MutablePage header;
    try {
      header = database.getTransaction().getPageToModify(new PageId(file.getFileId(), 0), pageSize, false);

      if (header.getAvailableContentSize() < Binary.SHORT_SERIALIZED_SIZE + property.length)
        throw new DatabaseMetadataException("No space left in dictionary file (items=" + itemCount + ")");

      header.writeString(header.getContentSize(), propertyName);

      itemCount++;

      updateCounters(header);
    } catch (IOException e) {
      throw new SchemaException("Error on adding new item to the database schema dictionary");
    }
  }

  private void updateCounters(final MutablePage header) {
    header.writeInt(DICTIONARY_ITEM_COUNT, itemCount);
  }
}
