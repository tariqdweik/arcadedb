/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Index cursor doesn't remove the deleted entries.
 */
public class IndexLSMTreeCursor implements IndexCursor {
  private final IndexLSMTree               index;
  private final Object[]                   toKeys;
  private final IndexLSMTreePageIterator[] pageIterators;
  private       IndexLSMTreePageIterator   currentIterator;
  private       Object[]                   currentKeys;
  private       Object[]                   currentValues;
  private       int                        currentValueIndex = 0;
  private       int                        totalPages;
  private       byte[]                     keyTypes;
  private final Object[][]                 keys;
  private       BinarySerializer           serializer;
  private       BinaryComparator           comparator;

  private int validIterators;

  public IndexLSMTreeCursor(final IndexLSMTree index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, null);
  }

  public IndexLSMTreeCursor(final IndexLSMTree index, final boolean ascendingOrder, final Object[] fromKeys, final Object[] toKeys) throws IOException {
    this.index = index;
    index.checkForNulls(fromKeys);
    this.toKeys = index.checkForNulls(toKeys);

    this.keyTypes = index.getKeyTypes();
    this.totalPages = index.getTotalPages();

    this.serializer = index.getDatabase().getSerializer();
    this.comparator = this.serializer.getComparator();

    // CREATE ITERATORS, ONE PER PAGE
    pageIterators = new IndexLSMTreePageIterator[totalPages];
    keys = new Object[totalPages][keyTypes.length];

    validIterators = 0;

    int pageId = ascendingOrder ? 0 : totalPages - 1;

    while (ascendingOrder ? pageId < totalPages : pageId >= 0) {
      keys[pageId] = null;

      if (fromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        final IndexLSMTree.LookupResult lookupResult;
        if (fromKeys == toKeys)
          // USE THE BLOOM FILTER
          lookupResult = index.searchInPage(currentPage, currentPageBuffer, fromKeys, count, ascendingOrder ? 2 : 3);
        else
          lookupResult = index.lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, fromKeys, ascendingOrder ? 1 : 2);

        if (lookupResult != null) {
          pageIterators[pageId] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);

          if (toKeys == null || (IndexLSMTree.compareKeys(comparator, keyTypes, pageIterators[pageId].getKeys(), toKeys) <= 0)) {
            keys[pageId] = pageIterators[pageId].getKeys();
            validIterators++;
          }
        }

      } else {
        if (ascendingOrder) {
          pageIterators[pageId] = index.newPageIterator(pageId, -1, ascendingOrder);
        } else {
          final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
          pageIterators[pageId] = index.newPageIterator(pageId, index.getCount(currentPage), ascendingOrder);
        }

        if (pageIterators[pageId].hasNext()) {
          pageIterators[pageId].next();

          if (toKeys == null || (IndexLSMTree.compareKeys(comparator, keyTypes, pageIterators[pageId].getKeys(), toKeys) <= 0)) {
            keys[pageId] = pageIterators[pageId].getKeys();
            validIterators++;
          }
        }
      }

      pageId += ascendingOrder ? 1 : -1;
    }
  }

  @Override
  public boolean hasNext() {
    return validIterators > 0 || (currentValues != null && currentValueIndex < currentValues.length);
  }

  @Override
  public Object next() {
    do {
      if (currentValues != null && currentValueIndex < currentValues.length) {
        final Object value = currentValues[currentValueIndex++];
        if (!index.isDeletedEntry(value))
          return value;

        continue;
      }

      currentValueIndex = 0;

      Object[] minorKey = null;
      int minorKeyIndex = -1;

      // FIND THE MINOR KEY
      for (int p = 0; p < totalPages; ++p) {
        if (minorKey == null) {
          minorKey = keys[p];
          minorKeyIndex = p;
        } else {
          if (keys[p] != null) {
            if (IndexLSMTree.compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
              minorKey = keys[p];
              minorKeyIndex = p;
            }
          }
        }
      }

      currentIterator = pageIterators[minorKeyIndex];
      currentKeys = currentIterator.getKeys();
      currentValues = currentIterator.getValue();

      if (currentIterator.hasNext()) {
        currentIterator.next();
        keys[minorKeyIndex] = currentIterator.getKeys();

        if (toKeys != null && (IndexLSMTree.compareKeys(comparator, keyTypes, keys[minorKeyIndex], toKeys) > 0)) {
          currentIterator.close();
          currentIterator = null;
          pageIterators[minorKeyIndex] = null;
          keys[minorKeyIndex] = null;
          --validIterators;
        }
      } else {
        currentIterator.close();
        currentIterator = null;
        pageIterators[minorKeyIndex] = null;
        keys[minorKeyIndex] = null;
        --validIterators;
      }
    } while ((currentValues == null || index.isDeletedEntry(currentValues[currentValueIndex])) && hasNext());

    return currentValues == null || currentValueIndex >= currentValues.length ? null : currentValues[currentValueIndex++];
  }

  @Override
  public Object[] getKeys() {
    return currentKeys;
  }

  @Override
  public void close() {
    for (IndexLSMTreePageIterator it : pageIterators)
      if (it != null)
        it.close();
    Arrays.fill(pageIterators, null);
  }
}
