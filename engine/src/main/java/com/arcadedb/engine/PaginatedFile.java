/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PaginatedFile {
  public enum MODE {
    READ_ONLY, READ_WRITE
  }

  private final String      filePath;
  private       FileChannel channel;
  private       int         fileId;
  private       int         pageSize;
  private       String      fileName;
  private final String      fileExtension;
  private       boolean     open;

  protected PaginatedFile(final String filePath, final MODE mode) throws FileNotFoundException {
    this.filePath = filePath;

    String filePrefix = filePath.substring(0, filePath.lastIndexOf("."));
    this.fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);

    final int pageSizePos = filePrefix.lastIndexOf(".");
    pageSize = Integer.parseInt(filePrefix.substring(pageSizePos + 1));
    filePrefix = filePrefix.substring(0, pageSizePos);

    final int fileIdPos = filePrefix.lastIndexOf(".");
    if (fileIdPos > -1) {
      fileId = Integer.parseInt(filePrefix.substring(fileIdPos + 1));
      int pos = filePrefix.lastIndexOf("/");
      fileName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      int pos = filePrefix.lastIndexOf("/");
      fileName = filePrefix.substring(pos + 1);
    }

    this.channel = new RandomAccessFile(filePath, mode == MODE.READ_WRITE ? "rw" : "r").getChannel();
    this.open = true;
  }

  public void close() throws IOException {
    channel.close();
    this.open = false;
  }

  public void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public void flush() throws IOException {
    channel.force(true);
  }

  /**
   * Returns the byte written. Current implementation flushes always the entire page because (1) there is not a sensible increase of
   * performance and (2) in case a page is modified multiple times before the flush now it's overwritten in the writeCache map.
   */
  public int write(final ModifiablePage page) throws IOException {
    if (page.pageId.getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to write: " + page.pageId.getPageNumber());

    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();

    buffer.rewind();
    channel.write(buffer, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
    return pageSize;
//
//    final int[] range = page.getModifiedRange();
//
//    assert range[0] > -1 && range[1] < pageSize;
//
//    if (range[0] == 0 && range[1] == pageSize - 1) {
//      // FLUSH THE ENTIRE PAGE
//      buffer.rewind();
//      channel.write(buffer, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
//      return pageSize;
//    }
//
//    // FLUSH ONLY THE UPDATED VERSION + DELTA
//    buffer.position(range[1] + 1);
//    buffer.flip();
//    buffer.position(0); // ALWAYS WRITE FROM 0 TO INCLUDE PAGE VERSION
//    final ByteBuffer delta = buffer.slice();
//
//    channel.write(delta, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
//
//    return range[1] - range[0] + 1;
  }

  public void read(final ImmutablePage page) throws IOException {
    if (page.pageId.getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to read: " + page.pageId.getPageNumber());

    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();
    buffer.clear();
    channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  public String getFileName() {
    return fileName;
  }

  public String getFileExtension() {
    return fileExtension;
  }

  public int getFileId() {
    return fileId;
  }

  public void setFileId(final int fileId) {
    this.fileId = fileId;
  }

  public int getPageSize() {
    return pageSize;
  }

  @Override
  public String toString() {
    return filePath;
  }

  public static String getFileNameFromPath(final String filePath) {
    final String filePrefix = filePath.substring(0, filePath.lastIndexOf("."));

    final String fileName;
    final int fileIdPos = filePrefix.lastIndexOf(".");
    if (fileIdPos > -1) {
      int pos = filePrefix.lastIndexOf("/");
      fileName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      int pos = filePrefix.lastIndexOf("/");
      fileName = filePrefix.substring(pos + 1);
    }
    return fileName;
  }
}
