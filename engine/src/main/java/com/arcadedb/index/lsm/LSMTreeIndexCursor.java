/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;

import java.io.IOException;
import java.util.*;

/**
 * Index cursor doesn't remove the deleted entries.
 */
public class LSMTreeIndexCursor implements IndexCursor {
  private final LSMTreeIndexMutable                    index;
  private final boolean                                ascendingOrder;
  private final Object[]                               toKeys;
  private final Object[]                               serializedToKeys;
  private final boolean                                toKeysInclusive;
  private final LSMTreeIndexUnderlyingAbstractCursor[] pageCursors;
  private       LSMTreeIndexUnderlyingAbstractCursor   currentCursor;
  private       Object[]                               currentKeys;
  private       RID[]                                  currentValues;
  private       int                                    currentValueIndex = 0;
  private final int                                    totalCursors;
  private       byte[]                                 keyTypes;
  private final Object[][]                             keys;
  private       BinarySerializer                       serializer;
  private       BinaryComparator                       comparator;

  private int validIterators;

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, true, null, true);
  }

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder, final Object[] fromKeys, final boolean beginKeysInclusive,
      final Object[] toKeys, final boolean endKeysInclusive) throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.keyTypes = index.getKeyTypes();

    final Object[] serializedFromKeys = index.convertKeys(index.checkForNulls(fromKeys), keyTypes);

    this.toKeys = toKeys;
    this.serializedToKeys = index.convertKeys(index.checkForNulls(toKeys), keyTypes);
    this.toKeysInclusive = endKeysInclusive;

    this.serializer = index.getDatabase().getSerializer();
    this.comparator = this.serializer.getComparator();

    final LSMTreeIndexCompacted compacted = index.getSubIndex();

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> compactedSeriesIterators;

    if (compacted != null)
      // INCLUDE COMPACTED
      compactedSeriesIterators = compacted.newIterators(ascendingOrder, serializedFromKeys);
    else
      compactedSeriesIterators = Collections.emptyList();

    final int totalPages = index.getTotalPages();

    totalCursors = compactedSeriesIterators.size() + totalPages;

    pageCursors = new LSMTreeIndexUnderlyingAbstractCursor[totalCursors];
    keys = new Object[totalCursors][keyTypes.length];

    validIterators = 0;

    for (int i = 0; i < compactedSeriesIterators.size(); ++i) {
      pageCursors[i] = compactedSeriesIterators.get(i);
      if (pageCursors[i] != null) {
        if (pageCursors[i].hasNext()) {
          pageCursors[i].next();
          keys[i] = pageCursors[i].getKeys();
        } else
          pageCursors[i] = null;
      }
    }

    for (int pageId = 0; pageId < totalPages; ++pageId) {
      final int cursorIdx = compactedSeriesIterators.size() + pageId;

      if (serializedFromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        if (count > 0) {
          LSMTreeIndexMutable.LookupResult lookupResult = index
              .lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer, serializedFromKeys, ascendingOrder ? 2 : 3);

          if (!lookupResult.outside) {
            pageCursors[cursorIdx] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);
            keys[cursorIdx] = pageCursors[cursorIdx].getKeys();

            if (ascendingOrder) {
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[cursorIdx], fromKeys) < 0) {
                pageCursors[cursorIdx] = null;
                keys[cursorIdx] = null;
              }
            } else {
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[cursorIdx], fromKeys) > 0) {
                pageCursors[cursorIdx] = null;
                keys[cursorIdx] = null;
              }
            }
          }
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
          keys[cursorIdx] = pageCursors[cursorIdx].getKeys();
        } else
          pageCursors[cursorIdx] = null;
      }
    }

    // CHECK THE VALIDITY OF CURSORS
    for (int i = 0; i < pageCursors.length; ++i) {
      final LSMTreeIndexUnderlyingAbstractCursor pageCursor = pageCursors[i];

      if (pageCursor != null) {
        if (fromKeys != null && !beginKeysInclusive) {
          if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[i], fromKeys) == 0) {
            // SKIP THIS
            if (pageCursor.hasNext()) {
              pageCursor.next();
              keys[i] = pageCursor.getKeys();
            } else
              // INVALID
              pageCursors[i] = null;
          }
        }

        if (this.serializedToKeys != null) {
          //final Object[] cursorKey = index.convertKeys(index.checkForNulls(pageCursor.getKeys()), keyTypes);
          final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, pageCursor.getKeys(), this.toKeys);

          if ((ascendingOrder && ((endKeysInclusive && compare <= 0) || (!endKeysInclusive && compare < 0))) || (!ascendingOrder && (
              (endKeysInclusive && compare >= 0) || (!endKeysInclusive && compare > 0))))
            ;
          else
            // INVALID
            pageCursors[i] = null;
        }

        if (pageCursors[i] != null) {
          final RID[] rids = pageCursors[i].getValue();
          if (rids != null && rids.length > 0) {
            boolean valid = true;
            for (RID r : rids) {
              if (r.getBucketId() < 0) {
                valid = false;
                break;
              }
            }
            if (valid)
              validIterators++;
          }
        }
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
  public long size() {
    return 0;
  }

  @Override
  public boolean hasNext() {
    return validIterators > 0 || (currentValues != null && currentValueIndex < currentValues.length);
  }

  @Override
  public RID next() {
    do {
      if (currentValues != null && currentValueIndex < currentValues.length) {
        final RID value = currentValues[currentValueIndex++];
        if (!index.isDeletedEntry(value))
          return value;

        continue;
      }

      currentValueIndex = 0;

      Object[] minorKey = null;
      final List<Integer> minorKeyIndexes = new ArrayList<>();

      // FIND THE MINOR KEY
      for (int p = 0; p < totalCursors; ++p) {
        if (pageCursors[p] != null) {
          if (minorKey == null) {
            minorKey = keys[p];
            minorKeyIndexes.add(p);
          } else {
            if (keys[p] != null) {
              final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[p], minorKey);
              if (compare == 0) {
                minorKeyIndexes.add(p);
              } else if ((ascendingOrder && compare < 0) || (!ascendingOrder && compare > 0)) {
                minorKey = keys[p];
                minorKeyIndexes.clear();
                minorKeyIndexes.add(p);
              }
            }
          }
        }
      }

      if (minorKeyIndexes.isEmpty())
        throw new NoSuchElementException();

      for (int i = 0; i < minorKeyIndexes.size(); ++i) {
        final int minorKeyIndex = minorKeyIndexes.get(i);

        currentCursor = pageCursors[minorKeyIndex];

        currentKeys = currentCursor.getKeys();

        final RID[] tempCurrentValues = currentCursor.getValue();

        if (i == 0)
          currentValues = tempCurrentValues;
        else {
          // MERGE VALUES
          final RID[] newArray = Arrays.copyOf(currentValues, currentValues.length + tempCurrentValues.length);
          for (int k = currentValues.length; k < newArray.length; ++k)
            newArray[k] = tempCurrentValues[k - currentValues.length];
          currentValues = newArray;
        }

        // FILTER DELETED ITEMS
        final Set<RID> removedRIDs = new HashSet<>();
        final Set<RID> validRIDs = new HashSet<>();

        // START FROM THE LAST ENTRY
        for (int k = currentValues.length - 1; k > -1; --k) {
          final RID rid = currentValues[k];

          if (LSMTreeIndexAbstract.REMOVED_ENTRY_RID.equals(rid))
            break;

          if (rid.getBucketId() < 0) {
            // RID DELETED, SKIP THE RID
            final RID originalRID = index.getOriginalRID(rid);
            if (!validRIDs.contains(originalRID))
              removedRIDs.add(originalRID);
            continue;
          }

          if (removedRIDs.contains(rid))
            // HAS BEEN DELETED
            continue;

          validRIDs.add(rid);
        }

        if (validRIDs.isEmpty())
          currentValues = null;
        else
          validRIDs.toArray(currentValues);

        // PREPARE THE NEXT ENTRY
        if (currentCursor.hasNext()) {
          currentCursor.next();
          keys[minorKeyIndex] = currentCursor.getKeys();

          if (serializedToKeys != null) {
            final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[minorKeyIndex], toKeys);

            if ((ascendingOrder && ((toKeysInclusive && compare > 0) || (!toKeysInclusive && compare >= 0))) || (!ascendingOrder && (
                (toKeysInclusive && compare < 0) || (!toKeysInclusive && compare <= 0)))) {
              currentCursor.close();
              currentCursor = null;
              pageCursors[minorKeyIndex] = null;
              keys[minorKeyIndex] = null;
              --validIterators;
            }
          }
        } else {
          currentCursor.close();
          currentCursor = null;
          pageCursors[minorKeyIndex] = null;
          keys[minorKeyIndex] = null;
          --validIterators;
        }
      }

    } while ((currentValues == null || currentValues.length == 0 || (currentValueIndex < currentValues.length && index
        .isDeletedEntry(currentValues[currentValueIndex]))) && hasNext());

    return currentValues == null || currentValueIndex >= currentValues.length ? null : currentValues[currentValueIndex++];
  }

  @Override
  public Object[] getKeys() {
    return currentKeys;
  }

  @Override
  public Identifiable getRecord() {
    if (currentValues != null && currentValueIndex < currentValues.length) {
      final RID value = currentValues[currentValueIndex];
      if (!index.isDeletedEntry(value))
        return value;
    }
    return null;
  }

  @Override
  public int getScore() {
    return 1;
  }

  @Override
  public void close() {
    for (LSMTreeIndexUnderlyingAbstractCursor it : pageCursors)
      if (it != null)
        it.close();
    Arrays.fill(pageCursors, null);
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
