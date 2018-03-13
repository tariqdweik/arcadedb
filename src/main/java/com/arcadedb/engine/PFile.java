package com.arcadedb.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PFile {
  public enum MODE {
    READ_ONLY, READ_WRITE
  }

  private final String      filePath;
  private       FileChannel channel;
  private       int         fileId;
  private       String      fileName;
  private final String      fileExtension;
  private       boolean     open;
  private final Lock lock = new ReentrantLock();

  protected PFile(final String filePath, final MODE mode) throws FileNotFoundException {
    this.filePath = filePath;

    final String filePrefix = filePath.substring(0, filePath.lastIndexOf("."));
    this.fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);

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

  public void write(final PModifiablePage page) throws IOException {
    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();
    buffer.rewind();
    channel.write(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
  }

  public void read(final PImmutablePage page) throws IOException {
    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();
    buffer.clear();
    channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
  }

  public boolean tryLock(final long timeInMs) throws InterruptedException {
    return lock.tryLock(timeInMs, TimeUnit.MILLISECONDS);
  }

  public void unlock() {
    lock.unlock();
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
