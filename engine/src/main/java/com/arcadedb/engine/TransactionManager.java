/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LogManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
  private static final long MAX_LOG_FILE_SIZE = 64 * 1024 * 1024;

  private final DatabaseInternal database;
  private       WALFile[]        activeWALFilePool;
  private final List<WALFile>    inactiveWALFilePool = new ArrayList<>();

  private final Timer          task;
  private       CountDownLatch taskExecuting = new CountDownLatch(0);

  private final AtomicLong transactionIds = new AtomicLong();
  private final AtomicLong logFileCounter = new AtomicLong();

  private AtomicLong statsPagesWritten = new AtomicLong();
  private AtomicLong statsBytesWritten = new AtomicLong();

  public TransactionManager(final DatabaseInternal database) {
    this.database = database;

    createFilePool();

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

  public void writeTransactionToWAL(final List<ModifiablePage> pages, final boolean sync) throws IOException {
    while (true) {
      final WALFile file = activeWALFilePool[(int) (Thread.currentThread().getId() % activeWALFilePool.length)];

      if (file != null && file.acquire(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          final long txId = transactionIds.getAndIncrement();
          file.writeTransaction(database, pages, sync, file, txId);
          return null;
        }
      }))
        return;

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  public void notifyPageFlushed(final ModifiablePage page) {
    final WALFile walFile = page.getWALFile();

    if (walFile == null)
      return;

    walFile.notifyPageFlushed();
  }

  public void checkIntegrity() {
    LogManager.instance().warn(this, "Started recovery of database '%s'", database);

    try {
      // OPEN EXISTENT WAL FILES
      final File dir = new File(database.getDatabasePath());
      final File[] walFiles = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".wal");
        }
      });

      if (walFiles.length == 0) {
        LogManager.instance().warn(this, "Recovery not possible because no WAL files were found");
        return;
      }

      activeWALFilePool = new WALFile[walFiles.length];
      for (int i = 0; i < walFiles.length; ++i) {
        try {
          activeWALFilePool[i] = new WALFile(database.getDatabasePath() + "/" + walFiles[i].getName());
        } catch (FileNotFoundException e) {
          LogManager.instance()
              .error(this, "Error on WAL file management for file '%s'", e, database.getDatabasePath() + walFiles[i].getName());
        }
      }

      if (activeWALFilePool != null) {
        final WALFile.WALTransaction[] walPositions = new WALFile.WALTransaction[activeWALFilePool.length];
        for (int i = 0; i < activeWALFilePool.length; ++i) {
          final WALFile file = activeWALFilePool[i];
          walPositions[i] = file.getFirstTransaction();
        }

        while (true) {
          int lowerTx = -1;
          long lowerTxId = -1;
          for (int i = 0; i < walPositions.length; ++i) {
            final WALFile.WALTransaction walTx = walPositions[i];
            if (walTx != null) {
              if (lowerTxId == -1 || walTx.txId < lowerTxId) {
                lowerTxId = walTx.txId;
                lowerTx = i;
              }
            }
          }

          if (lowerTxId == -1)
            // FINISHED
            break;

          applyChanges(walPositions[lowerTx]);

          walPositions[lowerTx] = activeWALFilePool[lowerTx].getTransaction(walPositions[lowerTx].endPositionInLog);
        }

        // REMOVE ALL WAL FILES
        for (int i = 0; i < activeWALFilePool.length; ++i) {
          final WALFile file = activeWALFilePool[i];
          try {
            file.drop();
            LogManager.instance().debug(this, "Dropped WAL file '%s'", file);
          } catch (IOException e) {
            LogManager.instance().error(this, "Error on dropping WAL file '%s'", e, file);
          }
        }
        createFilePool();
        database.getPageManager().clear();
      }
    } finally {
      LogManager.instance().warn(this, "Recovery of database '%s' completed", database);
    }
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("logFiles", logFileCounter.get());

    for (int i = 0; i < activeWALFilePool.length; ++i) {
      final WALFile file = activeWALFilePool[i];
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

  private boolean applyChanges(final WALFile.WALTransaction tx) {
    boolean changed = false;

    LogManager.instance().info(this, "- applying changes from txId=%d", tx.txId);

    for (WALFile.WALPage txPage : tx.pages) {
      final PaginatedFile file = database.getFileManager().getFile(txPage.fileId);

      final PageId pageId = new PageId(txPage.fileId, txPage.pageNumber);
      try {
        final BasePage page = database.getPageManager().getPage(pageId, file.getPageSize(), false);
        if (txPage.currentPageVersion < page.getVersion())
          // SKIP IT
          continue;

        if (txPage.currentPageVersion > page.getVersion() + 1)
          throw new WALException(
              "Cannot apply changes to the database because version (" + txPage.currentPageVersion + ") does not match (" + page
                  .getVersion() + ")");

        if (txPage.currentPageVersion != page.getVersion()) {
          final ModifiablePage modifiedPage = page.modify();
          txPage.currentContent.position(0);
          modifiedPage.writeByteArray(txPage.changesFrom - BasePage.PAGE_HEADER_SIZE, txPage.currentContent.getContent());
          modifiedPage.version = txPage.currentPageVersion;
          modifiedPage.setContentSize(txPage.currentPageSize);
          modifiedPage.flushMetadata();
          file.write(modifiedPage);
          file.flush();

          final PaginatedComponent component = database.getSchema().getFileById(txPage.fileId);
          if (component != null) {
            final int newPageCount = (int) (file.getSize() / file.getPageSize());
            if (newPageCount > component.pageCount.get())
              component.setPageCount(newPageCount);
          }

          changed = true;
          LogManager.instance().info(this, "  - updating page %s v%d", pageId, modifiedPage.version);
        }

      } catch (IOException e) {
        throw new WALException("Cannot load page " + pageId, e);
      }
    }
    return changed;
  }

  private void createFilePool() {
    activeWALFilePool = new WALFile[Runtime.getRuntime().availableProcessors()];
    for (int i = 0; i < activeWALFilePool.length; ++i) {
      try {
        activeWALFilePool[i] = new WALFile(database.getDatabasePath() + "/txlog_" + logFileCounter.getAndIncrement() + ".wal");
      } catch (FileNotFoundException e) {
        LogManager.instance().error(this, "Error on WAL file management for file '%s'", e,
            database.getDatabasePath() + "/txlog_" + logFileCounter.getAndIncrement() + ".wal");
      }
    }
  }

  private void checkWALFiles() {
    if (activeWALFilePool != null)
      for (int i = 0; i < activeWALFilePool.length; ++i) {
        final WALFile file = activeWALFilePool[i];
        try {
          if (file != null && file.getSize() > MAX_LOG_FILE_SIZE) {
            LogManager.instance()
                .debug(this, "WAL file '%s' reached maximum size (%d), set it as inactive, waiting for the drop", file,
                    MAX_LOG_FILE_SIZE);
            activeWALFilePool[i] = new WALFile(database.getDatabasePath() + "/txlog_" + logFileCounter.getAndIncrement() + ".wal");
            file.setActive(false);
            inactiveWALFilePool.add(file);
          }
        } catch (IOException e) {
          LogManager.instance().error(this, "Error on WAL file management for file '%s'", e, file);
        }
      }
  }

  private boolean cleanWALFiles() {
    for (Iterator<WALFile> it = inactiveWALFilePool.iterator(); it.hasNext(); ) {
      final WALFile file = it.next();

      LogManager.instance().debug(this, "Inactive file %s contains %d pending pages to flush", file, file.getPagesToFlush());

      if (file.getPagesToFlush() == 0) {
        // ALL PAGES FLUSHED, REMOVE THE FILE
        try {
          final Map<String, Object> fileStats = file.getStats();
          statsPagesWritten.addAndGet((Long) fileStats.get("pagesWritten"));
          statsBytesWritten.addAndGet((Long) fileStats.get("bytesWritten"));

          file.drop();

          LogManager.instance().debug(this, "Dropped WAL file '%s'", file);
        } catch (IOException e) {
          LogManager.instance().error(this, "Error on dropping WAL file '%s'", e, file);
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
