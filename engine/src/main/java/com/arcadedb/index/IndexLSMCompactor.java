/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.ModifiablePage;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.LogManager;

import java.io.IOException;

public class IndexLSMCompactor {
  private final IndexLSM index;

  public IndexLSMCompactor(final IndexLSM index) {
    this.index = index;
  }

  public void compact() throws IOException {
    final Database database = index.getDatabase();

    final int totalPages = index.getTotalPages();
    LogManager.instance().info(this, "Compacting index '%s' (pages=%d)...", index, totalPages);

    index.getDatabase().begin();
    final IndexLSM newIndex = index.copy();
    ((SchemaImpl) index.getDatabase().getSchema()).registerFile(newIndex);

    final byte[] keyTypes = index.getKeyTypes();

    final long indexCompactionRAM = GlobalConfiguration.INDEX_COMPACTION_RAM.getValueAsLong() * 1024 * 1024;

    long loops = 0;
    long totalKeys = 0;

    final Binary keyValueContent = new Binary();

    int pagesToCompact = 0;
    for (int pageIndex = 0; pageIndex < totalPages; ) {

      if ((totalPages - pageIndex) * (long) index.getPageSize() > indexCompactionRAM)
        pagesToCompact = (int) (indexCompactionRAM / index.getPageSize());
      else
        pagesToCompact = totalPages - pageIndex;

      final IndexLSMPageIterator[] iterators = new IndexLSMPageIterator[pagesToCompact];
      for (int i = 0; i < pagesToCompact; ++i)
        iterators[i] = index.newPageIterator(pageIndex + i, 0, true);

      final Object[][] keys = new Object[pagesToCompact][keyTypes.length];

      for (int p = 0; p < pagesToCompact; ++p) {
        if (iterators[p].hasNext()) {
          iterators[p].next();
          keys[p] = iterators[p].getKeys();
        } else {
          iterators[p].close();
          iterators[p] = null;
          keys[p] = null;
        }
      }

      final BinarySerializer serializer = database.getSerializer();
      final BinaryComparator comparator = serializer.getComparator();

      ModifiablePage lastPage = null;
      TrackableBinary currentPageBuffer = null;

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
              if (IndexLSM.compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
                minorKey = keys[p];
                minorKeyIndex = p;
              }
            }
          }
        }

        final Object value = iterators[minorKeyIndex].getValue();
        final ModifiablePage newPage = newIndex
            .appendDuringCompaction(keyValueContent, lastPage, currentPageBuffer, minorKey, (RID) value);
        if (newPage != lastPage) {
          currentPageBuffer = newPage.getTrackable();
          lastPage = newPage;
        }

        ++totalKeys;

        if (totalKeys % 1000000 == 0)
          LogManager.instance().info(this, "- keys %d - loops %d - page %s", totalKeys, loops, newPage);

        if (iterators[minorKeyIndex].hasNext()) {
          iterators[minorKeyIndex].next();
          keys[minorKeyIndex] = iterators[minorKeyIndex].getKeys();
        } else {
          iterators[minorKeyIndex].close();
          iterators[minorKeyIndex] = null;
          keys[minorKeyIndex] = null;
        }
      }

      LogManager.instance().info(this, "Compacted %s pages, total %d...", pagesToCompact, pageIndex + pagesToCompact);

      database.commit();
      database.begin();

      pageIndex += pagesToCompact;
    }

    // SWAP OLD WITH NEW INDEX
    // TODO
    ((SchemaImpl) database.getSchema()).swapIndexes(index, newIndex);

    database.commit();

    LogManager.instance().info(this, "Compaction completed for index '%s'. New File has %d ordered pages (%d iterations)", index,
        newIndex.getTotalPages(), loops);
  }
}
