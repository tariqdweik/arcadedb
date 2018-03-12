package com.arcadedb.engine;

import com.arcadedb.PGlobalConfiguration;
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

    final long indexCompactionRAM = PGlobalConfiguration.INDEX_COMPACTION_RAM.getValueAsLong() * 1024 * 1024;

    long loops = 0;
    long totalKeys = 0;

    final PBinary keyValueContent = new PBinary();

    int pagesToCompact = 0;
    for (int pageIndex = 0; pageIndex < totalPages; ) {

      if ((totalPages - pageIndex) * (long) index.getPageSize() > indexCompactionRAM)
        pagesToCompact = (int) (indexCompactionRAM / index.getPageSize());
      else
        pagesToCompact = totalPages - pageIndex;

      final PIndexPageIterator[] iterators = new PIndexPageIterator[pagesToCompact];
      for (int i = 0; i < pagesToCompact; ++i)
        iterators[i] = index.newIterator(pageIndex + i);

      final Object[][] keys = new Object[pagesToCompact][keyTypes.length];

      for (int p = 0; p < pagesToCompact; ++p) {
        if (iterators[p].hasNext())
          keys[p] = iterators[p].getKeys();
        else {
          iterators[p].close();
          iterators[p] = null;
          keys[p] = null;
        }
      }

      final PBinarySerializer serializer = index.database.getSerializer();
      final PBinaryComparator comparator = serializer.getComparator();

      PModifiablePage lastPage = null;
      PBinary currentPageBuffer = null;

      boolean moreItems = true;
      for (; moreItems; ++loops) {
        moreItems = false;

        Object[] minorKey = null;
        int minorKeyIndex = -1;

        // FIND THE MINOR KEY
        for (int p = 0; p < pagesToCompact; ++p) {
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
        final PModifiablePage newPage = newIndex
            .appendDuringCompaction(keyValueContent, lastPage, currentPageBuffer, minorKey, (PRID) value);
        if (newPage != lastPage) {
          currentPageBuffer = new PBinary(newPage.slice());
          lastPage = newPage;
        }

        ++totalKeys;

        if (totalKeys % 1000000 == 0)
          PLogManager.instance().info(this, "- keys %d - loops %d - page %s", totalKeys, loops, newPage);

        if (iterators[minorKeyIndex].hasNext()) {
          iterators[minorKeyIndex].next();
          keys[minorKeyIndex] = iterators[minorKeyIndex].getKeys();
        } else {
          iterators[minorKeyIndex].close();
          iterators[minorKeyIndex] = null;
          keys[minorKeyIndex] = null;
        }
      }

      PLogManager.instance().info(this, "Compacted %s pages, total %d...", pagesToCompact, pageIndex + pagesToCompact);

      index.database.commit();
      index.database.begin();

      pageIndex += pagesToCompact;
    }

    // SWAP OLD WITH NEW INDEX
    // TODO
    ((PSchemaImpl) index.database.getSchema()).swapIndexes(index, newIndex);

    index.database.commit();

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
