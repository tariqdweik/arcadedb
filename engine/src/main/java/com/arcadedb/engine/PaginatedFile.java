/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.log.LogManager;

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

  private final MODE        mode;
  private       String      filePath;
  private       String      fileName;
  private       FileChannel channel;
  private       int         fileId;
  private       int         pageSize;
  private       String      componentName;
  private       String      fileExtension;
  private       boolean     open;

  public PaginatedFile() {
    this.mode = MODE.READ_ONLY;
  }

  protected PaginatedFile(final String filePath, final MODE mode) throws FileNotFoundException {
    this.mode = mode;
    open(filePath, mode);
  }

  public void close() {
    try {
      channel.close();
    } catch (IOException e) {
      LogManager.instance().error(this, "Error on closing file %s (id=%d)", e, filePath, fileId);
    }
    this.open = false;
  }

  public void rename(final String newFileName) throws FileNotFoundException {
    close();

    final int pos = filePath.indexOf(fileName);
    final String dir = filePath.substring(0, pos);

    final File newFile = new File(dir + "/" + newFileName);
    new File(filePath).renameTo(newFile);

    open(newFile.getAbsolutePath(), mode);
  }

  public void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public long getTotalPages() throws IOException {
    return channel.size() / pageSize;
  }

  public void flush() throws IOException {
    channel.force(true);
  }

  public String getFileName() {
    return fileName;
  }

  /**
   * Returns the byte written. Current implementation flushes always the entire page because (1) there is not a sensible increase of
   * performance and (2) in case a page is modified multiple times before the flush now it's overwritten in the writeCache map.
   */
  public int write(final MutablePage page) throws IOException {
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
//    buffer.rewind(); // ALWAYS WRITE FROM 0 TO INCLUDE PAGE VERSION
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

  public String getComponentName() {
    return componentName;
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

  private void open(final String filePath, final MODE mode) throws FileNotFoundException {
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
      componentName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      int pos = filePrefix.lastIndexOf("/");
      componentName = filePrefix.substring(pos + 1);
    }

    final int lastSlash = filePath.lastIndexOf("/");
    if (lastSlash > -1)
      fileName = filePath.substring(lastSlash + 1);
    else
      fileName = filePath;

    this.channel = new RandomAccessFile(filePath, mode == MODE.READ_WRITE ? "rw" : "r").getChannel();
    this.open = true;
  }
}
