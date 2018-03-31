package com.arcadedb.database;

import com.arcadedb.engine.PBasePage;
import com.arcadedb.engine.PModifiablePage;
import com.arcadedb.engine.PWALFile;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PPair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PTransactionManager {
  private static final int TX_HEADER_SIZE = PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;
  private static final int TX_FOOTER_SIZE = PBinary.LONG_SERIALIZED_SIZE;

  private static final int PAGE_HEADER_SIZE =
      PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinary.LONG_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE
          + PBinary.INT_SERIALIZED_SIZE;
  private static final int PAGE_FOOTER_SIZE = PBinary.INT_SERIALIZED_SIZE;

  private static final long MAGIC_NUMBER      = 9371515385058702l;
  private static final long MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;

  private final PDatabaseInternal database;
  private       PWALFile[]        activeWALFilePool;
  private final List<PWALFile> inactiveWALFilePool = new ArrayList<>();
  private       AtomicInteger  walFilePoolCursor   = new AtomicInteger();

  private final Timer task;
  private CountDownLatch taskExecuting = new CountDownLatch(0);

  private final AtomicLong transactionIds = new AtomicLong();
  private final AtomicLong logFileCounter = new AtomicLong();

  private final AtomicLong statsPageWritten  = new AtomicLong();
  private final AtomicLong statsBytesWritten = new AtomicLong();

  protected PTransactionManager(final PDatabaseInternal database) {
    this.database = database;

    task = new Timer();
    task.schedule(new TimerTask() {
      @Override
      public void run() {
        if (activeWALFilePool != null) {
          taskExecuting = new CountDownLatch(1);
          try {
            checkWALFiles();
            cleanWALFiles();
          } finally {
            taskExecuting.countDown();
          }
        }
      }
    }, 1000, 1000);
  }

  public void close() {
    if (task != null)
      task.cancel();

    try {
      taskExecuting.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }

    if (activeWALFilePool != null) {
      // MOVE ALL WAL FILES AS INACTIVE
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        inactiveWALFilePool.add(activeWALFilePool[i]);
        activeWALFilePool[i] = null;
      }

      while (!cleanWALFiles()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  public void writeTransactionToWAL(final List<PPair<PBasePage, PModifiablePage>> pages, final boolean sync) throws IOException {
    final PWALFile file = acquireWALFile();

    final long txId = transactionIds.getAndIncrement();

    // WRITE TX HEADER (TXID, PAGES)
    byte[] buffer = new byte[TX_HEADER_SIZE];
    PBinary binary = new PBinary(buffer, TX_HEADER_SIZE);
    binary.putLong(txId);
    binary.putInt(pages.size());
    file.append(binary.buffer);

    statsBytesWritten.addAndGet(TX_HEADER_SIZE);

    // WRITE ALL PAGES SEGMENTS
    for (PPair<PBasePage, PModifiablePage> entry : pages) {
      final PBasePage prevPage = entry.getFirst();
      final PModifiablePage newPage = entry.getSecond();

      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      final int segmentSize = PAGE_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2)) + PAGE_FOOTER_SIZE;

      buffer = new byte[segmentSize];
      binary = new PBinary(buffer, segmentSize);

      binary.putInt(segmentSize);
      binary.putInt(newPage.getPageId().getFileId());
      binary.putLong(newPage.getPageId().getPageNumber());
      binary.putInt(deltaRange[0]);
      binary.putInt(deltaRange[1]);
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
      binary.position(PAGE_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2)));
      binary.putInt(segmentSize);

      file.appendPage(binary.buffer);

      statsPageWritten.incrementAndGet();
      statsBytesWritten.addAndGet(segmentSize);
    }

    // WRITE TX FOOTER (MAGIC NUMBER)
    buffer = new byte[TX_FOOTER_SIZE];
    binary = new PBinary(buffer, TX_FOOTER_SIZE);
    binary.putLong(MAGIC_NUMBER);

    binary.reset();
    file.append(binary.buffer);

    statsBytesWritten.addAndGet(TX_FOOTER_SIZE);

    if (sync)
      file.flush();
  }

  public void notifyPageFlushed(final PModifiablePage page) {
    final PWALFile walFile = page.getWALFile();

    if (walFile == null)
      return;

    walFile.notifyPageFlushed();
  }

  public void checkIntegrity() {

  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("pagesWritten", statsPageWritten.get());
    map.put("bytesWritten", statsBytesWritten.get());
    map.put("logFiles", logFileCounter.get());
    return map;
  }

  /**
   * Returns the next file from the pool. If it's null (temporary moved to inactive) the next not-null is taken.
   */
  private PWALFile acquireWALFile() {
    if (activeWALFilePool == null)
      createFilePool();

    int pos = walFilePoolCursor.getAndIncrement();
    if (pos >= activeWALFilePool.length) {
      walFilePoolCursor.set(0);
      pos = 0;
    }

    return activeWALFilePool[pos];
  }

  private void createFilePool() {
    activeWALFilePool = new PWALFile[Runtime.getRuntime().availableProcessors()];
    for (int i = 0; i < activeWALFilePool.length; ++i)
      activeWALFilePool[i] = newWALFile();
  }

  private PWALFile newWALFile() {
    final String fileName = database.getDatabasePath() + "/txlog_" + logFileCounter.getAndIncrement() + ".wal";
    try {
      return new PWALFile(fileName);
    } catch (FileNotFoundException e) {
      throw new PDatabaseMetadataException("Cannot create WAL file " + fileName, e);
    }
  }

  private void checkWALFiles() {
    for (int i = 0; i < activeWALFilePool.length; ++i) {
      final PWALFile file = activeWALFilePool[i];
      try {
        if (file.getSize() > MAX_LOG_FILE_SIZE) {
          PLogManager.instance()
              .debug(this, "WAL file %s reached maximum size (%d), set it as inactive, waiting for the drop", file,
                  MAX_LOG_FILE_SIZE);
          activeWALFilePool[i] = newWALFile();
          inactiveWALFilePool.add(file);
        }
      } catch (IOException e) {
        PLogManager.instance().error(this, "Error on WAL file management for file %s", e, file);
      }
    }
  }

  private boolean cleanWALFiles() {
    for (Iterator<PWALFile> it = inactiveWALFilePool.iterator(); it.hasNext(); ) {
      final PWALFile file = it.next();

      PLogManager.instance().debug(this, "Inactive file %s contains %d pending pages to flush", file, file.getPagesToFlush());

      if (file.getPagesToFlush() == 0) {
        // ALL PAGES FLUSHED, REMOVE THE FILE
        try {
          file.drop();
          PLogManager.instance().debug(this, "Dropped WAL file %s", file);
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on dropping WAL file %s", e, file);
        }
        it.remove();
      }
    }

    return inactiveWALFilePool.isEmpty();
  }
}
