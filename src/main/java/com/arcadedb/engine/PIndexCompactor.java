package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PRID;
import com.arcadedb.schema.PSchemaImpl;
import com.arcadedb.serializer.PBinaryComparator;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.utility.PLogManager;

import java.io.IOException;

public class PIndexCompactor {
  private final PIndex index;

  public PIndexCompactor(final PIndex index) {
    this.index = index;
  }

  public void compact() throws IOException {
    index.flush();

    final int totalPages = index.getTotalPages();
    PLogManager.instance().info(this, "Compacting index '%s' (pages=%d)...", index, totalPages);

    index.database.begin();
    final PIndex newIndex = index.copy();
    ((PSchemaImpl) index.database.getSchema()).registerFile(newIndex);

    final byte[] keyTypes = index.getKeyTypes();

    final PIndexIterator[] iterators = new PIndexIterator[totalPages];
    for (int i = 0; i < totalPages; ++i)
      iterators[i] = index.newIterator(i);

    final Object[][] keys = new Object[totalPages][keyTypes.length];

    for (int p = 0; p < totalPages; ++p) {
      if (iterators[p].hasNext())
        keys[p] = iterators[p].getKeys();
      else
        keys[p] = null;
    }

    final PBinarySerializer serializer = index.database.getSerializer();
    final PBinaryComparator comparator = serializer.getComparator();

    PModifiablePage lastPage = null;
    PBinary currentPageBuffer = null;

    boolean moreItems = true;
    int loops = 0;
    for (; moreItems; ++loops) {
      moreItems = false;

      Object[] minorKey = null;
      int minorKeyIndex = -1;

      // FIND THE MINOR KEY
      for (int p = 0; p < totalPages; ++p) {
        if (minorKey == null) {
          minorKey = keys[p];
          minorKeyIndex = p;
        } else {
          if (keys[p] != null) {
            moreItems = true;
            if (compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
              minorKey = keys[p];
              minorKeyIndex = p;
            }
          }
        }
      }

      final Object value = iterators[minorKeyIndex].getValue();
      final PModifiablePage newPage = newIndex.appendDuringCompaction(lastPage, currentPageBuffer, minorKey, (PRID) value);
      if (lastPage == null)
        currentPageBuffer = new PBinary(newPage.slice());
      lastPage = newPage;

      if (iterators[minorKeyIndex].hasNext()) {
        iterators[minorKeyIndex].next();
        keys[minorKeyIndex] = iterators[minorKeyIndex].getKeys();
      } else
        keys[minorKeyIndex] = null;
    }

    // SWAP OLD WITH NEW INDEX
    // TODO
    ((PSchemaImpl) index.database.getSchema()).swapIndexes(index, newIndex);

    index.database.commit();

    newIndex.flush();

    PLogManager.instance().info(this, "Compaction completed for index '%s'. New File has %d ordered pages (%d iterations)", index,
        newIndex.getTotalPages(), loops);
  }

  private int compareKeys(final PBinaryComparator comparator, final byte[] keyTypes, final Object[] keys1, final Object[] keys2) {
    for (int k = 0; k < keyTypes.length; ++k) {
      final int result = comparator.compare(keys1[k], keyTypes[k], keys2[k], keyTypes[k]);
      if (result < 0)
        return -1;
      else if (result > 0)
        return 1;
    }
    return 0;
  }
}
