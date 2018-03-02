package com.arcadedb.engine;

/**
 * Immutable.
 */
public class PPageId {
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
}
