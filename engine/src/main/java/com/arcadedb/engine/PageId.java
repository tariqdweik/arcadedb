/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

/**
 * Immutable.
 */
public class PageId implements Comparable<PageId> {
  private int fileId;
  private int pageNumber;

  public PageId(final int fileId, final int pageNumber) {
    this.fileId = fileId;
    this.pageNumber = pageNumber;
  }

  public int getFileId() {
    return fileId;
  }

  public int getPageNumber() {
    return pageNumber;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PageId pageId = (PageId) o;

    if (fileId != pageId.fileId)
      return false;
    return pageNumber == pageId.pageNumber;
  }

  @Override
  public int hashCode() {
    int result = fileId;
    result = 31 * result + pageNumber;
    return result;
  }

  @Override
  public String toString() {
    return "fileId=" + fileId + " pageNumber=" + pageNumber;
  }

  @Override
  public int compareTo(final PageId o) {
    if( o == this )
      return 0;

    if (!(o instanceof PageId))
      throw new IllegalArgumentException("cannot compare a page id with " + o.getClass());

    if (fileId > o.fileId)
      return 1;
    else if (fileId < o.fileId)
      return -1;

    if (pageNumber > o.pageNumber)
      return 1;
    else if (pageNumber < o.pageNumber)
      return -1;
    return 0;
  }
}
