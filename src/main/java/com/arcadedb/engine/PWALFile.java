package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.PRID;
import com.arcadedb.utility.PLockContext;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PPair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PWALFile extends PLockContext {
  // TXID (long) + PAGES (int) + SEGMENT_SIZE (int)
  private static final int TX_HEADER_SIZE =
      PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;
  // SEGMENT_SIZE (int) + MAGIC_NUMBER (long)
  private static final int TX_FOOTER_SIZE = PBinary.INT_SERIALIZED_SIZE + PBinary.LONG_SERIALIZED_SIZE;

  // FILE_ID (int) + FILE_POSITION (long) + DELTA_FROM (int) + DELTA_TO (int) + EXISTS_PREVIOUS (byte) + CURR_PAGE_VERSION (int)
  private static final int PAGE_HEADER_SIZE =
      PBinary.INT_SERIALIZED_SIZE + PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE
          + PBinary.BYTE_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;

  private static final long MAGIC_NUMBER = 9371515385058702l;

  private final String      filePath;
  private       FileChannel channel;
  private       boolean     open;
  private AtomicInteger pagesToFlush = new AtomicInteger();

  private long statsPagesWritten = 0;
  private long statsBytesWritten = 0;

  // STATIC BUFFERS USED FOR RECOVERY
  private final ByteBuffer bufferLong = ByteBuffer.allocate(PBinary.LONG_SERIALIZED_SIZE);
  private final ByteBuffer bufferInt  = ByteBuffer.allocate(PBinary.INT_SERIALIZED_SIZE);
  private final ByteBuffer bufferByte = ByteBuffer.allocate(PBinary.BYTE_SERIALIZED_SIZE);

  public class WALTransaction {
    public long      txId;
    public WALPage[] pages;
    public long      startPositionInLog;
    public long      endPositionInLog;
  }

  public class WALPage {
    public PRID    rid;
    public int     changesFrom;
    public int     changesTo;
    public PBinary previousContent;
    public PBinary currentContent;
    public int     currentPageVersion;
  }

  public PWALFile(final String filePath) throws FileNotFoundException {
    super(true);
    this.filePath = filePath;
    this.channel = new RandomAccessFile(filePath, "rw").getChannel();
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

  public void flush() throws IOException {
    channel.force(false);
  }

  public WALTransaction getLastTransaction(final PDatabaseInternal database) throws PWALException {
    try {
      final long fileSize = channel.size();
      if (fileSize < TX_HEADER_SIZE)
        return null;

      long pos = fileSize;

      long mn = readLong(pos - PBinary.LONG_SERIALIZED_SIZE);
      if (mn != MAGIC_NUMBER) {
        // INVALID RECORD, START FROM THE BEGINNING
        long lastGoodTxPos = -1;
        pos = 0;
        while (pos < fileSize) {
          final int segmentSize = readInt(pos += PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE);
          pos += PBinary.INT_SERIALIZED_SIZE + segmentSize;
          final int segmentSize2 = readInt(pos);
          if (segmentSize2 != segmentSize)
            // INVALID TX SEGMENT, GET PREVIOUS AS GOOD;
            break;

          pos += PBinary.INT_SERIALIZED_SIZE;

          mn = readLong(pos);
          if (mn != MAGIC_NUMBER)
            break;

          pos += PBinary.LONG_SERIALIZED_SIZE;
        }

        if (lastGoodTxPos == -1)
          return null;
        else
          return getTransaction(database, lastGoodTxPos);

      } else {
        // GOOD RECORD
        pos -= PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;
        final int segmentSize = readInt(pos);
        if (segmentSize < PAGE_HEADER_SIZE || segmentSize > pos)
          throw new PWALException("Invalid segmentSize " + segmentSize + " in WAL record (position=" + pos + ")");

        pos -= segmentSize + PBinary.INT_SERIALIZED_SIZE;
        final int segmentSize2 = readInt(pos);
        if (segmentSize2 != segmentSize)
          throw new PWALException(
              "Invalid segmentSize " + segmentSize2 + " in WAL record (position=" + pos + ") because is different than "
                  + segmentSize);

        return getTransaction(database, pos - PBinary.INT_SERIALIZED_SIZE - PBinary.LONG_SERIALIZED_SIZE);
      }

    } catch (IOException e) {
      PLogManager.instance().error(this, "Error on reading last transaction from WAL '%s'", e, filePath);
      throw new PWALException("Error on reading last transaction from WAL '" + filePath + "'", e);
    }
  }

  private WALTransaction getTransaction(final PDatabaseInternal database, long pos) throws IOException {
    final WALTransaction tx = new WALTransaction();

    tx.startPositionInLog = pos;

    tx.txId = readLong(pos);
    pos += PBinary.LONG_SERIALIZED_SIZE;

    final int pages = readInt(pos);
    pos += PBinary.INT_SERIALIZED_SIZE;

    final int segmentSize = readInt(pos);
    pos += PBinary.INT_SERIALIZED_SIZE;

    tx.pages = new WALPage[pages];

    for (int i = 0; i < pages; ++i) {
      tx.pages[i] = new WALPage();
      tx.pages[i].rid = new PRID(database, readInt(pos), readLong(pos + PBinary.INT_SERIALIZED_SIZE));

      pos += PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesFrom = readInt(pos);
      pos += PBinary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesTo = readInt(pos);
      pos += PBinary.INT_SERIALIZED_SIZE;

      final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

      final boolean hasPrevious = readByte(pos) == 1;
      pos += PBinary.BYTE_SERIALIZED_SIZE;

      tx.pages[i].currentPageVersion = readInt(pos);
      pos += PBinary.INT_SERIALIZED_SIZE;

      if (hasPrevious) {
        tx.pages[i].previousContent = new PBinary(deltaSize);
        channel.read(tx.pages[i].previousContent.getByteBuffer(), pos);

        pos += deltaSize;
      }

      tx.pages[i].currentContent = new PBinary(deltaSize);
      channel.read(tx.pages[i].currentContent.getByteBuffer(), pos);

      pos += deltaSize;
    }

    tx.endPositionInLog = pos + PBinary.INT_SERIALIZED_SIZE + PBinary.LONG_SERIALIZED_SIZE;

    return tx;
  }

  public synchronized void writeTransaction(final PDatabaseInternal database, final List<PPair<PBasePage, PModifiablePage>> pages,
      final boolean sync, final PWALFile file, final long txId) throws IOException {
    // WRITE TX HEADER (TXID, PAGES)
    byte[] buffer = new byte[TX_HEADER_SIZE];
    PBinary pageBuffer = new PBinary(buffer, TX_HEADER_SIZE);
    pageBuffer.putLong(txId);
    pageBuffer.putInt(pages.size());

    // COMPUTE TOTAL TXLOG SEGMENT SIZE
    int segmentSize = 0;
    for (PPair<PBasePage, PModifiablePage> entry : pages) {
      final PBasePage prevPage = entry.getFirst();
      final PModifiablePage newPage = entry.getSecond();

      final int[] deltaRange = newPage.getModifiedRange();
      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      segmentSize += PAGE_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2));
    }

    pageBuffer.putInt(segmentSize);
    file.append(pageBuffer.getByteBuffer());

    statsBytesWritten += TX_HEADER_SIZE;

    // WRITE ALL PAGES SEGMENTS
    int currentPage = 0;
    for (PPair<PBasePage, PModifiablePage> entry : pages) {
      final PBasePage prevPage = entry.getFirst();
      final PModifiablePage newPage = entry.getSecond();

      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      final int pageSize = PAGE_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2));
      buffer = new byte[pageSize];
      pageBuffer = new PBinary(buffer, pageSize);

      pageBuffer.putInt(newPage.getPageId().getFileId());
      pageBuffer.putLong(newPage.getPageId().getPageNumber());
      pageBuffer.putInt(deltaRange[0]);
      pageBuffer.putInt(deltaRange[1]);
      pageBuffer.putByte((byte) (prevPage != null ? 1 : 0));
      pageBuffer.putInt(newPage.version);
      if (prevPage != null) {
        final ByteBuffer prevPageBuffer = prevPage.getContent();
        prevPageBuffer.position(deltaRange[0]);
        prevPageBuffer.get(buffer, PAGE_HEADER_SIZE, deltaSize);

        final ByteBuffer newPageBuffer = newPage.getContent();
        newPageBuffer.position(deltaRange[0]);
        newPageBuffer.get(buffer, PAGE_HEADER_SIZE + deltaSize, deltaSize);
      } else {
        final ByteBuffer newPageBuffer = newPage.getContent();
        newPageBuffer.position(deltaRange[0]);
        newPageBuffer.get(buffer, PAGE_HEADER_SIZE, deltaSize);
      }
      pageBuffer.reset();

      file.appendPage(pageBuffer.getByteBuffer());

      statsPagesWritten++;
      statsBytesWritten += pageSize;

      currentPage++;

      if (currentPage == pages.size())
        database.executeCallbacks(PDatabaseInternal.CALLBACK_EVENT.TX_LAST_OP);
    }

    // WRITE TX FOOTER (MAGIC NUMBER)
    buffer = new byte[TX_FOOTER_SIZE];
    pageBuffer = new PBinary(buffer, TX_FOOTER_SIZE);
    pageBuffer.putInt(segmentSize);
    pageBuffer.putLong(MAGIC_NUMBER);

    pageBuffer.reset();
    file.append(pageBuffer.getByteBuffer());

    statsBytesWritten += TX_FOOTER_SIZE;

    if (sync)
      file.flush();

    database.executeCallbacks(PDatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE);
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

  public void appendPage(final ByteBuffer buffer) throws IOException {
    pagesToFlush.incrementAndGet();
    append(buffer);
  }

  public void append(final ByteBuffer buffer) throws IOException {
    buffer.rewind();
    channel.write(buffer, channel.size());
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
}
