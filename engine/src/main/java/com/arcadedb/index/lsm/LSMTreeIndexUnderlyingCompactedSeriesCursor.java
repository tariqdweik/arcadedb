/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;

import java.io.IOException;

public class LSMTreeIndexUnderlyingCompactedSeriesCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  private final int                              lastPageNumber;
  private       LSMTreeIndexUnderlyingPageCursor pageCursor;

  public LSMTreeIndexUnderlyingCompactedSeriesCursor(final LSMTreeIndexCompacted index, final int firstPageNumber, final int lastPageNumber,
      final byte[] keyTypes, final boolean ascendingOrder, final int posInPage) {
    super(index, keyTypes, keyTypes.length, ascendingOrder);
    this.lastPageNumber = lastPageNumber;

    loadNextNonEmptyPage(firstPageNumber, posInPage);
  }

  @Override
  public boolean hasNext() {
    if (pageCursor == null)
      return false;

    if (pageCursor.hasNext())
      return true;

    final int nextPage = pageCursor.pageId.getPageNumber() + (ascendingOrder ? 1 : -1);

    loadNextNonEmptyPage(nextPage, -1);

    return pageCursor.hasNext();
  }

  private void loadNextNonEmptyPage(final int startingPageNumber, int posInPage) {
    // LOAD NEXT PAGE IF NEEDED
    for (int currentPageNumber = startingPageNumber; ascendingOrder ?
        currentPageNumber <= lastPageNumber :
        currentPageNumber >= lastPageNumber; currentPageNumber += ascendingOrder ? 1 : -1) {
      try {
        final BasePage page = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), currentPageNumber), index.getPageSize());
        final int count = index.getCount(page);

        pageCursor = new LSMTreeIndexUnderlyingPageCursor(index, page, posInPage == -1 ? ascendingOrder ? -1 : count : posInPage,
            index.getHeaderSize(currentPageNumber), keyTypes, count, ascendingOrder);

        if (pageCursor.hasNext())
          break;

        // RESET POSITION IN PAGE AFTER THE 1ST PAGE
        posInPage = -1;

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
  public RID[] getValue() {
    return pageCursor.getValue();
  }

  @Override
  public PageId getCurrentPageId() {
    return pageCursor.pageId;
  }

  @Override
  public int getCurrentPositionInPage() {
    return pageCursor.currentEntryIndex;
  }
}
