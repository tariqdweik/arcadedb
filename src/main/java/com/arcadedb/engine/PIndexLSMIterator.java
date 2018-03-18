package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.serializer.PBinaryComparator;
import com.arcadedb.serializer.PBinarySerializer;

import java.io.IOException;
import java.util.Arrays;

public class PIndexLSMIterator implements PIndexIterator {
  private final PIndexLSM            index;
  private final boolean              ascendingOrder;
  private final Object[]             fromKeys;
  private final Object[]             toKeys;
  private final PIndexPageIterator[] pageIterators;
  private       PIndexPageIterator   currentIterator;
  private       Object[]             currentKeys;
  private       Object               currentValue;
  private       int                  totalPages;
  private       byte[]               keyTypes;
  private final Object[][]           keys;
  private       PBinarySerializer    serializer;
  private       PBinaryComparator    comparator;

  private int validIterators;

  public PIndexLSMIterator(final PIndexLSM index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, null);
  }

  public PIndexLSMIterator(final PIndexLSM index, final boolean ascendingOrder, final Object[] fromKeys, final Object[] toKeys)
      throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.fromKeys = fromKeys;
    this.toKeys = toKeys;

    this.keyTypes = index.getKeyTypes();
    this.totalPages = index.pageCount;

    this.serializer = index.database.getSerializer();
    this.comparator = this.serializer.getComparator();

    // CREATE ITERATORS, ONE PER PAGE
    pageIterators = new PIndexPageIterator[totalPages];

    int pageId = ascendingOrder ? 0 : totalPages - 1;

    while (ascendingOrder ? pageId < totalPages : pageId >= 0) {
      if (fromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final PBasePage currentPage = index.database.getTransaction()
            .getPage(new PPageId(index.file.getFileId(), pageId), index.pageSize);
        final PBinary currentPageBuffer = new PBinary(currentPage.slice());
        final int count = index.getCount(currentPage);

        final PIndexLSM.LookupResult lookupResult = index.lookup(pageId, count, currentPageBuffer, fromKeys);
        pageIterators[pageId] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);
      } else {
        if (ascendingOrder) {
          pageIterators[pageId] = index.newPageIterator(pageId, -1, ascendingOrder);
        } else {
          final PBasePage currentPage = index.database.getTransaction()
              .getPage(new PPageId(index.file.getFileId(), pageId), index.pageSize);
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
      final PIndexPageIterator it = pageIterators[p];
      if (it != null) {
        if (it.hasNext()) {
          keys[p] = it.getKeys();

          if (toKeys != null && (PIndexLSM.compareKeys(comparator, keyTypes, keys[p], toKeys) > 0)) {
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
          if (PIndexLSM.compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
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

      if (toKeys != null && (PIndexLSM.compareKeys(comparator, keyTypes, keys[minorKeyIndex], toKeys) > 0)) {
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
    for (PIndexPageIterator it : pageIterators)
      if (it != null)
        it.close();
    Arrays.fill(pageIterators, null);
  }
}
