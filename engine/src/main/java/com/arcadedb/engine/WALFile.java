/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LockContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class WALFile extends LockContext {
  // TXID (long) + PAGES (int) + SEGMENT_SIZE (int)
  private static final int TX_HEADER_SIZE = Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  // SEGMENT_SIZE (int) + MAGIC_NUMBER (long)
  private static final int TX_FOOTER_SIZE = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  // FILE_ID (int) + PAGE_NUMBER (int) + DELTA_FROM (int) + DELTA_TO (int) + CURR_PAGE_VERSION (int)+ CURR_PAGE_SIZE (int)
  private static final int PAGE_HEADER_SIZE =
      Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE
          + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;

  public static final long MAGIC_NUMBER = 9371515385058702l;

  private final    String        filePath;
  private          FileChannel   channel;
  private volatile boolean       active       = true;
  private volatile boolean       open;
  private          AtomicInteger pagesToFlush = new AtomicInteger();

  private long statsPagesWritten = 0;
  private long statsBytesWritten = 0;

  // STATIC BUFFERS USED FOR RECOVERY
  private final ByteBuffer bufferLong = ByteBuffer.allocate(Binary.LONG_SERIALIZED_SIZE);
  private final ByteBuffer bufferInt  = ByteBuffer.allocate(Binary.INT_SERIALIZED_SIZE);
  private final ByteBuffer bufferByte = ByteBuffer.allocate(Binary.BYTE_SERIALIZED_SIZE);

  public static class WALTransaction {
    public long      txId;
    public WALPage[] pages;
    public long      startPositionInLog;
    public long      endPositionInLog;
  }

  public static class WALPage {
    public int    fileId;
    public int    pageNumber;
    public int    changesFrom;
    public int    changesTo;
    public Binary currentContent;
    public int    currentPageVersion;
    public int    currentPageSize;
  }

  public WALFile(final String filePath) throws FileNotFoundException {
    super(true);
    this.filePath = filePath;
    this.channel = new RandomAccessFile(filePath, "rw").getChannel();
    this.open = true;
  }

  public synchronized void close() throws IOException {
    this.open = false;
    channel.close();
  }

  public synchronized void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public WALTransaction getFirstTransaction() throws WALException {
    return getTransaction(0);
  }

  /**
   * If the WAL is still active, execute the callback. This avoids to close a file where a thread is still writing to it.
   *
   * @return true if acquired, otherwise false
   */
  public synchronized boolean acquire(final Callable<Object> callable) {
    if (!active || !open)
      return false;

    try {
      callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new WALException("Error on writing to WAL file " + getFilePath(), e);
    }

    return true;
  }

  public boolean isActive() {
    return active;
  }

  public synchronized void setActive(final boolean active) {
    this.active = active;
  }

  public WALTransaction getTransaction(long pos) {
    final WALTransaction tx = new WALTransaction();

    tx.startPositionInLog = pos;

    try {
      if (pos + TX_HEADER_SIZE + TX_FOOTER_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      tx.txId = readLong(pos);
      pos += Binary.LONG_SERIALIZED_SIZE;

      final int pages = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int segmentSize = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      tx.pages = new WALPage[pages];

      for (int i = 0; i < pages; ++i) {
        if (pos > getSize())
          // INVALID
          return null;

        tx.pages[i] = new WALPage();

        tx.pages[i].fileId = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].pageNumber = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesFrom = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesTo = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

        tx.pages[i].currentPageVersion = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].currentPageSize = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        final ByteBuffer buffer = ByteBuffer.allocate(deltaSize);

        tx.pages[i].currentContent = new Binary(buffer);
        channel.read(buffer, pos);

        pos += deltaSize;
      }

      final long mn = readLong(pos + Binary.INT_SERIALIZED_SIZE);
      if (mn != MAGIC_NUMBER)
        // INVALID
        return null;

      tx.endPositionInLog = pos + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

      return tx;
    } catch (Exception e) {
      return null;
    }
  }

  public Binary writeTransaction(final DatabaseInternal database, final List<ModifiablePage> pages, final boolean sync,
      final WALFile file, final long txId) throws IOException {
    // WRITE TX HEADER (TXID, PAGES)
    byte[] buffer = new byte[TX_HEADER_SIZE];
    Binary pageBuffer = new Binary(buffer, TX_HEADER_SIZE);
    pageBuffer.putLong(txId);
    pageBuffer.putInt(pages.size());

    // COMPUTE TOTAL TXLOG SEGMENT SIZE
    int segmentSize = 0;
    for (ModifiablePage newPage : pages) {
      final int[] deltaRange = newPage.getModifiedRange();
      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      segmentSize += PAGE_HEADER_SIZE + deltaSize;
    }

    pageBuffer.putInt(segmentSize);
    file.append(pageBuffer.getByteBuffer());

    statsBytesWritten += TX_HEADER_SIZE;

    // WRITE ALL PAGES SEGMENTS
    int currentPage = 0;
    for (ModifiablePage newPage : pages) {
      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      final int pageSize = PAGE_HEADER_SIZE + deltaSize;
      buffer = new byte[pageSize];
      pageBuffer = new Binary(buffer, pageSize);

      pageBuffer.putInt(newPage.getPageId().getFileId());
      pageBuffer.putInt(newPage.getPageId().getPageNumber());
      pageBuffer.putInt(deltaRange[0]);
      pageBuffer.putInt(deltaRange[1]);
      pageBuffer.putInt(newPage.version + 1);
      pageBuffer.putInt(newPage.getContentSize());
      final ByteBuffer newPageBuffer = newPage.getContent();
      newPageBuffer.position(deltaRange[0]);
      newPageBuffer.get(buffer, PAGE_HEADER_SIZE, deltaSize);
      pageBuffer.position(0);

      file.appendPage(pageBuffer.getByteBuffer());

      statsPagesWritten++;
      statsBytesWritten += pageSize;

      currentPage++;

      if (currentPage == pages.size())
        database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.TX_LAST_OP);
    }

    // WRITE TX FOOTER (MAGIC NUMBER)
    buffer = new byte[TX_FOOTER_SIZE];
    pageBuffer = new Binary(buffer, TX_FOOTER_SIZE);
    pageBuffer.putInt(segmentSize);
    pageBuffer.putLong(MAGIC_NUMBER);

    pageBuffer.reset();
    file.append(pageBuffer.getByteBuffer());

    statsBytesWritten += TX_FOOTER_SIZE;

    if (sync)
      channel.force(false);

    database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE);

    return null;
  }

  public int getPagesToFlush() {
    return pagesToFlush.get();
  }

  public void notifyPageFlushed() {
    pagesToFlush.decrementAndGet();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  protected void appendPage(final ByteBuffer buffer) throws IOException {
    pagesToFlush.incrementAndGet();
    append(buffer);
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("pagesWritten", statsPagesWritten);
    map.put("bytesWritten", statsBytesWritten);
    return map;
  }

  private long readLong(final long pos) throws IOException {
    bufferLong.position(0);
    channel.read(bufferLong, pos);
    return bufferLong.getLong(0);
  }

  private int readInt(final long pos) throws IOException {
    bufferInt.position(0);
    channel.read(bufferInt, pos);
    return bufferInt.getInt(0);
  }

  private byte readByte(final long pos) throws IOException {
    bufferByte.position(0);
    channel.read(bufferByte, pos);
    return bufferByte.get(0);
  }

  protected void append(final ByteBuffer buffer) throws IOException {
    buffer.rewind();
    channel.write(buffer, channel.size());
  }
}
