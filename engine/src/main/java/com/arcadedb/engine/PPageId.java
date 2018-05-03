package com.arcadedb.engine;

/**
 * Immutable.
 */
public class PPageId implements Comparable<PPageId> {
  private int fileId;
  private int pageNumber;

  public PPageId(final int fileId, final int pageNumber) {
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

    final PPageId pPageId = (PPageId) o;

    if (fileId != pPageId.fileId)
      return false;
    return pageNumber == pPageId.pageNumber;
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
  public int compareTo(final PPageId o) {
    if( o == this )
      return 0;

    if (!(o instanceof PPageId))
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
