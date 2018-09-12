/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;

import java.io.IOException;

public class LSMTreeIndexUnderlyingCompactedSeriesCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  private final int                              lastPageNumber;
  private       LSMTreeIndexUnderlyingPageCursor pageCursor;

  public LSMTreeIndexUnderlyingCompactedSeriesCursor(final LSMTreeIndexCompacted index, final int firstPageNumber, final int lastPageNumber,
      final byte[] keyTypes, final boolean ascendingOrder) {
    super(index, keyTypes, keyTypes.length, ascendingOrder);
    this.lastPageNumber = lastPageNumber;

    loadNextNonEmptyPage(firstPageNumber);
  }

  @Override
  public boolean hasNext() {
    if (pageCursor.hasNext())
      return true;

    loadNextNonEmptyPage(pageCursor.pageId.getPageNumber() + 1);

    return pageCursor.hasNext();
  }

  private void loadNextNonEmptyPage(final int startingPageNumber) {
    // LOAD NEXT PAGE IF NEEDED
    for (int currentPageNumber = startingPageNumber; currentPageNumber <= lastPageNumber; ++currentPageNumber) {
      try {
        final BasePage page = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), currentPageNumber), index.getPageSize());
        final int count = index.getCount(page);
        pageCursor = new LSMTreeIndexUnderlyingPageCursor(index, page, ascendingOrder ? -1 : count, index.getHeaderSize(currentPageNumber), keyTypes, count,
            ascendingOrder);

        if (pageCursor.hasNext()) {
          pageCursor.next();
          break;
        }

      } catch (IOException e) {
        throw new IndexException("Error on iterating cursor on compacted index", e);
      }
    }
  }

  @Override
  public void next() {
    pageCursor.next();
  }

  @Override
  public Object[] getKeys() {
    return pageCursor.getKeys();
  }

  @Override
  public Object[] getValue() {
    return pageCursor.getValue();
  }
}
