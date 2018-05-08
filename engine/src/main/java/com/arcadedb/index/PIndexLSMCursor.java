package com.arcadedb.index;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;

import java.io.IOException;
import java.util.Arrays;

public class PIndexLSMCursor implements PIndexCursor {
  private final IndexLSM                index;
  private final boolean                 ascendingOrder;
  private final Object[]                fromKeys;
  private final Object[]                toKeys;
  private final PIndexLSMPageIterator[] pageIterators;
  private       PIndexLSMPageIterator   currentIterator;
  private       Object[]                currentKeys;
  private       Object                  currentValue;
  private       int                     totalPages;
  private       byte[]                  keyTypes;
  private final Object[][]              keys;
  private       BinarySerializer        serializer;
  private       BinaryComparator        comparator;

  private int validIterators;

  public PIndexLSMCursor(final IndexLSM index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, null);
  }

  public PIndexLSMCursor(final IndexLSM index, final boolean ascendingOrder, final Object[] fromKeys, final Object[] toKeys)
      throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.fromKeys = fromKeys;
    this.toKeys = toKeys;

    this.keyTypes = index.getKeyTypes();
    this.totalPages = index.getTotalPages();

    this.serializer = index.getDatabase().getSerializer();
    this.comparator = this.serializer.getComparator();

    // CREATE ITERATORS, ONE PER PAGE
    pageIterators = new PIndexLSMPageIterator[totalPages];

    int pageId = ascendingOrder ? 0 : totalPages - 1;

    while (ascendingOrder ? pageId < totalPages : pageId >= 0) {
      if (fromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = index.getDatabase().getTransaction()
            .getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        final IndexLSM.LookupResult lookupResult;
        if (fromKeys == toKeys)
          // USE THE BLOOM FILTER
          lookupResult = index.searchInPage(currentPage, currentPageBuffer, fromKeys, count, ascendingOrder ? 1 : 2);
        else
          lookupResult = index
              .lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, fromKeys, ascendingOrder ? 1 : 2);

        if (lookupResult != null)
          pageIterators[pageId] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);

      } else {
        if (ascendingOrder) {
          pageIterators[pageId] = index.newPageIterator(pageId, -1, ascendingOrder);
        } else {
          final BasePage currentPage = index.getDatabase().getTransaction()
              .getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
          pageIterators[pageId] = index.newPageIterator(pageId, index.getCount(currentPage), ascendingOrder);
        }

        if (pageIterators[pageId].hasNext())
          pageIterators[pageId].next();
      }

      pageId += ascendingOrder ? 1 : -1;
    }

    keys = new Object[totalPages][keyTypes.length];

    // CHECK ALL THE ITERATORS (NULL=SKIP)
    validIterators = 0;
    for (int p = 0; p < totalPages; ++p) {
      final PIndexLSMPageIterator it = pageIterators[p];
      if (it != null) {
        if (it.hasNext()) {
          keys[p] = it.getKeys();

          if (toKeys != null && (IndexLSM.compareKeys(comparator, keyTypes, keys[p], toKeys) > 0)) {
            it.close();
            pageIterators[p] = null;
            keys[p] = null;
          } else
            validIterators++;
        } else {
          it.close();
          pageIterators[p] = null;
          keys[p] = null;
        }
      } else
        keys[p] = null;
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    return validIterators > 0;
  }

  @Override
  public void next() throws IOException {
    Object[] minorKey = null;
    int minorKeyIndex = -1;

    // FIND THE MINOR KEY
    for (int p = 0; p < totalPages; ++p) {
      if (minorKey == null) {
        minorKey = keys[p];
        minorKeyIndex = p;
      } else {
        if (keys[p] != null) {
          if (IndexLSM.compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
            minorKey = keys[p];
            minorKeyIndex = p;
          }
        }
      }
    }

    currentIterator = pageIterators[minorKeyIndex];
    currentKeys = currentIterator.getKeys();
    currentValue = currentIterator.getValue();

    if (currentIterator.hasNext()) {
      currentIterator.next();
      keys[minorKeyIndex] = currentIterator.getKeys();

      if (toKeys != null && (IndexLSM.compareKeys(comparator, keyTypes, keys[minorKeyIndex], toKeys) > 0)) {
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
  }

  @Override
  public Object[] getKeys() {
    return currentKeys;
  }

  @Override
  public Object getValue() {
    return currentValue;
  }

  @Override
  public void close() {
    for (PIndexLSMPageIterator it : pageIterators)
      if (it != null)
        it.close();
    Arrays.fill(pageIterators, null);
  }
}
