package com.arcadedb.engine;

import java.io.IOException;

public class PIndexIterator {
  private final PIndexLSM          index;
  private final boolean            ascendingOrder;
  private       PIndexPageIterator page;
  private       int                currentPageId;

  public PIndexIterator(final PIndexLSM index, final int currentPageId, final int currentEntryInPage, final boolean ascendingOrder)
      throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.currentPageId = currentPageId;
    this.page = index.newPageIterator(currentPageId, currentEntryInPage, ascendingOrder);
  }

  public boolean hasNext() throws IOException {
    if (page.hasNext())
      return true;

    if (ascendingOrder) {
      if (currentPageId >= index.pageCount - 1)
        return false;

      ++currentPageId;
      page = index.newPageIterator(currentPageId, -1, ascendingOrder);
    } else {
      if (currentPageId == 0)
        return false;

      --currentPageId;
      final PBasePage newPage = index.database.getTransaction()
          .getPage(new PPageId(index.file.getFileId(), currentPageId), index.pageSize);
      page = index.newPageIterator(currentPageId, index.getCount(newPage), ascendingOrder);
    }

    return page.hasNext();
  }

  public void next() throws IOException {
    if (ascendingOrder) {
      if (page.getCurrentPosition() > page.getTotalEntries() - 1) {
        ++currentPageId;
        page = index.newPageIterator(currentPageId, -1, ascendingOrder);
      }
    } else {
      if (page.getCurrentPosition() <= 0) {
        --currentPageId;
        final PBasePage newPage = index.database.getTransaction()
            .getPage(new PPageId(index.file.getFileId(), currentPageId), index.pageSize);
        page = index.newPageIterator(currentPageId, index.getCount(newPage), ascendingOrder);
      }
    }

    page.next();
  }

  public Object[] getKeys() {
    return page.getKeys();
  }

  public Object getValue() {
    return page.getValue();

  }

  public void close() {
    page.close();
  }
}
