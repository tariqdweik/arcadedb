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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Index cursor doesn't remove the deleted entries.
 */
public class LSMTreeIndexCursor implements IndexCursor {
  private final LSMTreeIndexMutable                    index;
  private final Object[]                               toKeys;
  private final LSMTreeIndexUnderlyingAbstractCursor[] pageCursors;
  private       LSMTreeIndexUnderlyingAbstractCursor   currentCursor;
  private       Object[]                               currentKeys;
  private       Object[]                               currentValues;
  private       int                                    currentValueIndex = 0;
  private final int                                    totalCursors;
  private       byte[]                                 keyTypes;
  private final Object[][]                             keys;
  private       BinarySerializer                       serializer;
  private       BinaryComparator                       comparator;

  private int validIterators;

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, null);
  }

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder, Object[] fromKeys, final Object[] toKeys) throws IOException {
    this.index = index;
    this.keyTypes = index.getKeyTypes();

    fromKeys = index.convertKeys(index.checkForNulls(fromKeys), keyTypes);
    this.toKeys = index.convertKeys(index.checkForNulls(toKeys), keyTypes);

    this.serializer = index.getDatabase().getSerializer();
    this.comparator = this.serializer.getComparator();

    final LSMTreeIndexCompacted compacted = index.getSubIndex();

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> compactedSeriesIterators;

    if (compacted != null)
      // INCLUDE COMPACTED
      compactedSeriesIterators = compacted.newIterators(ascendingOrder, fromKeys);
    else
      compactedSeriesIterators = Collections.emptyList();

    final int totalPages = index.getTotalPages();

    totalCursors = compactedSeriesIterators.size() + totalPages;

    pageCursors = new LSMTreeIndexUnderlyingAbstractCursor[totalCursors];
    keys = new Object[totalCursors][keyTypes.length];

    validIterators = 0;

    for (int i = 0; i < compactedSeriesIterators.size(); ++i) {
      pageCursors[i] = compactedSeriesIterators.get(i);
      if (pageCursors[i].hasNext()) {
        pageCursors[i].next();
        keys[i] = pageCursors[i].getKeys();
        validIterators++;
      } else
        pageCursors[i] = null;
    }

    for (int pageId = 0; pageId < totalPages; ++pageId) {
      final int cursorIdx = compactedSeriesIterators.size() + pageId;

      keys[cursorIdx] = null;

      if (fromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        final LSMTreeIndexMutable.LookupResult lookupResult = index
            .lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, fromKeys, ascendingOrder ? 2 : 3);

        pageCursors[cursorIdx] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);

        if (toKeys == null || (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, pageCursors[pageId].getKeys(), toKeys) <= 0)) {
          keys[cursorIdx] = pageCursors[cursorIdx].getKeys();
          validIterators++;
        }

      } else {
        if (ascendingOrder) {
          pageCursors[cursorIdx] = index.newPageIterator(pageId, -1, ascendingOrder);
        } else {
          final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
          pageCursors[cursorIdx] = index.newPageIterator(pageId, index.getCount(currentPage), ascendingOrder);
        }

        if (pageCursors[cursorIdx].hasNext()) {
          pageCursors[cursorIdx].next();

          if (toKeys == null || (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, pageCursors[cursorIdx].getKeys(), toKeys) <= 0)) {
            keys[cursorIdx] = pageCursors[cursorIdx].getKeys();
            validIterators++;
          }
        } else
          pageCursors[cursorIdx] = null;
      }
    }
  }

  @Override
  public String dumpStats() {
    final StringBuilder buffer = new StringBuilder(1024);

    buffer.append(String.format("\nDUMP OF %s UNDERLYING-CURSORS on index %s", pageCursors.length, index.getName()));
    for (int i = 0; i < pageCursors.length; ++i) {
      final LSMTreeIndexUnderlyingAbstractCursor cursor = pageCursors[i];

      if (cursor == null)
        buffer.append(String.format("\n- Cursor[%d] = null", i));
      else {
        buffer.append(String.format("\n- Cursor[%d] %s=%s index=%s compacted=%s totalKeys=%d ascending=%s keyTypes=%s currentPageId=%s currentPosInPage=%d", i,
            Arrays.toString(keys[i]), Arrays.toString(cursor.getValue()), cursor.index, cursor instanceof LSMTreeIndexUnderlyingCompactedSeriesCursor,
            cursor.totalKeys, cursor.ascendingOrder, Arrays.toString(cursor.keyTypes), cursor.getCurrentPageId(), cursor.getCurrentPositionInPage()));
      }
    }

    return buffer.toString();
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
      for (int p = 0; p < totalCursors; ++p) {
        if (pageCursors[p] != null) {
          if (minorKey == null) {
            minorKey = keys[p];
            minorKeyIndex = p;
          } else {
            if (keys[p] != null) {
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[p], minorKey) < 0) {
                minorKey = keys[p];
                minorKeyIndex = p;
              }
            }
          }
        }
      }

      if (minorKeyIndex < 0)
        throw new NoSuchElementException();

      currentCursor = pageCursors[minorKeyIndex];
      currentKeys = currentCursor.getKeys();
      currentValues = currentCursor.getValue();

      if (currentCursor.hasNext()) {
        currentCursor.next();
        keys[minorKeyIndex] = currentCursor.getKeys();

        if (toKeys != null && (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[minorKeyIndex], toKeys) > 0)) {
          currentCursor.close();
          currentCursor = null;
          pageCursors[minorKeyIndex] = null;
          keys[minorKeyIndex] = null;
          --validIterators;
        }
      } else {
        currentCursor.close();
        currentCursor = null;
        pageCursors[minorKeyIndex] = null;
        keys[minorKeyIndex] = null;
        --validIterators;
      }
    } while ((currentValues == null || currentValues.length == 0 || index.isDeletedEntry(currentValues[currentValueIndex])) && hasNext());

    return currentValues == null || currentValueIndex >= currentValues.length ? null : currentValues[currentValueIndex++];
  }

  @Override
  public Object[] getKeys() {
    return currentKeys;
  }

  @Override
  public void close() {
    for (LSMTreeIndexUnderlyingAbstractCursor it : pageCursors)
      if (it != null)
        it.close();
    Arrays.fill(pageCursors, null);
  }
}
