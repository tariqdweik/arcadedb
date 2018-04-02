package com.arcadedb.database;

import com.arcadedb.engine.PBasePage;
import com.arcadedb.engine.PModifiablePage;
import com.arcadedb.engine.PWALFile;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PPair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PTransactionManager {
  private static final long MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;

  private final PDatabaseInternal database;
  private       PWALFile[]        activeWALFilePool;
  private final List<PWALFile> inactiveWALFilePool = new ArrayList<>();
  private       AtomicInteger  walFilePoolCursor   = new AtomicInteger();

  private final Timer task;
  private CountDownLatch taskExecuting = new CountDownLatch(0);

  private final AtomicLong transactionIds = new AtomicLong();
  private final AtomicLong logFileCounter = new AtomicLong();

  private AtomicLong statsPagesWritten = new AtomicLong();
  private AtomicLong statsBytesWritten = new AtomicLong();

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
    file.writeTransaction(database, pages, sync, file, txId);
  }

  public void notifyPageFlushed(final PModifiablePage page) {
    final PWALFile walFile = page.getWALFile();

    if (walFile == null)
      return;

    walFile.notifyPageFlushed();
  }

  public void checkIntegrity() {
    // OPEN EXISTENT WAL FILES
    for (int i = 0; ; ++i) {
      final String fileName = database.getDatabasePath() + "/txlog_" + logFileCounter.get() + ".wal";
      final File file = new File(fileName);
      if (!file.exists())
        break;

      if (activeWALFilePool == null)
        activeWALFilePool = new PWALFile[1];
      else
        activeWALFilePool = Arrays.copyOf(activeWALFilePool, activeWALFilePool.length + 1);

      activeWALFilePool[i] = newWALFile();
    }

    if (activeWALFilePool != null) {
      final PWALFile.WALTransaction[] walPositions = new PWALFile.WALTransaction[activeWALFilePool.length];
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final PWALFile file = activeWALFilePool[i];
        walPositions[i] = file.getLastTransaction(database);
      }

      // REMOVE ALL WAL FILES
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final PWALFile file = activeWALFilePool[i];
        try {
          file.drop();
          PLogManager.instance().debug(this, "Dropped WAL file '%s'", file);
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on dropping WAL file '%s'", e, file);
        }
      }
      activeWALFilePool = null;
    }
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("logFiles", logFileCounter.get());

    for (int i = 0; i < activeWALFilePool.length; ++i) {
      final PWALFile file = activeWALFilePool[i];
      if (file != null) {
        final Map<String, Object> stats = file.getStats();
        statsPagesWritten.addAndGet((Long) stats.get("pagesWritten"));
        statsBytesWritten.addAndGet((Long) stats.get("bytesWritten"));
      }
    }

    map.put("pagesWritten", statsPagesWritten.get());
    map.put("bytesWritten", statsBytesWritten.get());
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
      throw new PDatabaseMetadataException("Cannot create WAL file '" + fileName + "'", e);
    }
  }

  private void checkWALFiles() {
    for (int i = 0; i < activeWALFilePool.length; ++i) {
      final PWALFile file = activeWALFilePool[i];
      try {
        if (file.getSize() > MAX_LOG_FILE_SIZE) {
          PLogManager.instance()
              .debug(this, "WAL file '%s' reached maximum size (%d), set it as inactive, waiting for the drop", file,
                  MAX_LOG_FILE_SIZE);
          activeWALFilePool[i] = newWALFile();
          inactiveWALFilePool.add(file);
        }
      } catch (IOException e) {
        PLogManager.instance().error(this, "Error on WAL file management for file '%s'", e, file);
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
          final Map<String, Object> fileStats = file.getStats();
          statsPagesWritten.addAndGet((Long) fileStats.get("pagesWritten"));
          statsBytesWritten.addAndGet((Long) fileStats.get("bytesWritten"));

          file.drop();

          PLogManager.instance().debug(this, "Dropped WAL file '%s'", file);
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on dropping WAL file '%s'", e, file);
        }
        it.remove();
      }
    }

    return inactiveWALFilePool.isEmpty();
  }

  public void kill() {
    if (task != null) {
      task.cancel();
      task.purge();
    }

    try {
      taskExecuting.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }
  }
}
