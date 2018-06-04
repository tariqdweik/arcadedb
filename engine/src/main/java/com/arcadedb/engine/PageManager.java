/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.LockContext;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages pages from disk to RAM. Each page can have different size.
 */
public class PageManager extends LockContext {
  private final FileManager                           fileManager;
  private final ConcurrentMap<PageId, ImmutablePage>  readCache  = new ConcurrentHashMap<>(65536);
  private final ConcurrentMap<PageId, ModifiablePage> writeCache = new ConcurrentHashMap<>(65536);

  private final LockManager<Integer, Thread> lockManager      = new LockManager();
  private final TransactionManager           txManager;
  private       boolean                      flushOnlyAtClose = GlobalConfiguration.FLUSH_ONLY_AT_CLOSE.getValueAsBoolean();

  private final long       maxRAM;
  private final AtomicLong totalReadCacheRAM                     = new AtomicLong();
  private final AtomicLong totalWriteCacheRAM                    = new AtomicLong();
  private final AtomicLong totalPagesRead                        = new AtomicLong();
  private final AtomicLong totalPagesReadSize                    = new AtomicLong();
  private final AtomicLong totalPagesWritten                     = new AtomicLong();
  private final AtomicLong totalPagesWrittenSize                 = new AtomicLong();
  private final AtomicLong cacheHits                             = new AtomicLong();
  private final AtomicLong cacheMiss                             = new AtomicLong();
  private final AtomicLong totalConcurrentModificationExceptions = new AtomicLong();

  private       long                   lastCheckForRAM = 0;
  private final PageManagerFlushThread flushThread;

  public class PPageManagerStats {
    public long maxRAM;
    public long readCacheRAM;
    public long writeCacheRAM;
    public long pagesRead;
    public long pagesReadSize;
    public long pagesWritten;
    public long pagesWrittenSize;
    public int  pageFlushQueueLength;
    public long cacheHits;
    public long cacheMiss;
    public long concurrentModificationExceptions;
  }

  public PageManager(final FileManager fileManager, final TransactionManager txManager) {
    super(true);

    this.fileManager = fileManager;
    this.txManager = txManager;

    maxRAM = GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong() * 1024;
    if (maxRAM < 0)
      throw new ConfigurationException(GlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + maxRAM + ")");
    flushThread = new PageManagerFlushThread(this);
    flushThread.start();
  }

  public void close() {
    if (flushThread != null) {
      try {
        flushThread.close();
        flushThread.join();
      } catch (InterruptedException e) {
      }
    }

    // FLUSH REMAINING PAGES
    final boolean flushOnlyAtCloseOld = flushOnlyAtClose;
    flushOnlyAtClose = true;
    for (ModifiablePage p : writeCache.values()) {
      try {
        flushPage(p);
      } catch (Exception e) {
        LogManager.instance()
            .error(this, "Error on flushing page %s at closing (threadId=%d)", e, p, Thread.currentThread().getId());
      }
    }
    writeCache.clear();
    readCache.clear();
    totalReadCacheRAM.set(0);
    totalWriteCacheRAM.set(0);
    lockManager.close();

    flushOnlyAtClose = flushOnlyAtCloseOld;
  }

  /**
   * Test only API.
   */
  public void kill() {
    if (flushThread != null) {
      try {
        flushThread.close();
        flushThread.join();
      } catch (InterruptedException e) {
      }
    }

    writeCache.clear();
    readCache.clear();
    totalReadCacheRAM.set(0);
    totalWriteCacheRAM.set(0);
    lockManager.close();
  }

  public void clear() {
    readCache.clear();
    totalReadCacheRAM.set(0);
  }

  public void deleteFile(final int fileId) {
    for (Iterator<ImmutablePage> it = readCache.values().iterator(); it.hasNext(); ) {
      final ImmutablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalReadCacheRAM.addAndGet(-1 * p.getPhysicalSize());
        it.remove();
      }
    }

    for (Iterator<ModifiablePage> it = writeCache.values().iterator(); it.hasNext(); ) {
      final ModifiablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalWriteCacheRAM.addAndGet(-1 * p.getPhysicalSize());
        it.remove();
      }
    }
  }

  public BasePage getPage(final PageId pageId, final int pageSize, final boolean isNew) throws IOException {
    BasePage page = writeCache.get(pageId);
    if (page != null)
      cacheHits.incrementAndGet();
    else {
      page = readCache.get(pageId);
      if (page == null) {
        page = loadPage(pageId, pageSize);
        if (!isNew)
          cacheMiss.incrementAndGet();

      } else {
        cacheHits.incrementAndGet();
        page.updateLastAccesses();
      }

      if (page == null)
        throw new IllegalArgumentException(
            "Page id '" + pageId + "' does not exists (threadId=" + Thread.currentThread().getId() + ")");
    }

    return page;
  }

  public BasePage checkPageVersion(final ModifiablePage page, final boolean isNew) throws IOException {
    final BasePage p = getPage(page.getPageId(), page.getPhysicalSize(), isNew);

    if (p != null && p.getVersion() != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + page.getPageId() + " (current v." + page.getVersion() + " <> database v." + p
              .getVersion() + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
    }

    if (p != null)
      return p.createImmutableView();

    return null;
  }

  public void updatePages(final Map<PageId, ModifiablePage> newPages, final Map<PageId, ModifiablePage> modifiedPages)
      throws IOException, InterruptedException {
    lock();
    try {
      if (newPages != null)
        for (ModifiablePage p : newPages.values())
          updatePage(p, true);

      for (ModifiablePage p : modifiedPages.values())
        updatePage(p, false);
    } finally {
      unlock();
    }
  }

  public void updatePage(final ModifiablePage page, final boolean isNew) throws IOException, InterruptedException {
    final BasePage p = getPage(page.getPageId(), page.getPhysicalSize(), isNew);
    if (p != null) {

      if (p.getVersion() != page.getVersion()) {
        totalConcurrentModificationExceptions.incrementAndGet();
        throw new ConcurrentModificationException(
            "Concurrent modification on page " + page.getPageId() + " (current v." + page.getVersion() + " <> database v." + p
                .getVersion() + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
      }

      page.incrementVersion();
      page.flushMetadata();

      // ADD THE PAGE IN THE WRITE CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED DIRECTLY, SO IT CAN BE SHARED
      if (writeCache.put(page.pageId, page) == null)
        totalWriteCacheRAM.addAndGet(page.getPhysicalSize());

      if (!flushOnlyAtClose)
        // ONLY IF NOT ALREADY IN THE QUEUE, ENQUEUE THE PAGE TO BE FLUSHED BY A SEPARATE THREAD
        flushThread.asyncFlush(page);

      LogManager.instance()
          .debug(this, "Updated page %s (size=%d threadId=%d)", page, page.getPhysicalSize(), Thread.currentThread().getId());
    }
  }

  public void overridePage(final ModifiablePage page) throws IOException {
    // ADD THE PAGE IN THE WRITE CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED DIRECTLY, SO IT CAN BE SHARED
    if (writeCache.put(page.pageId, page) == null)
      totalWriteCacheRAM.addAndGet(page.getPhysicalSize());

    flushPage(page);

    LogManager.instance()
        .debug(this, "Overwritten page %s (size=%d threadId=%d)", page, page.getPhysicalSize(), Thread.currentThread().getId());
  }

  public List<Integer> tryLockFiles(final List<Integer> orderedModifiedFiles, final long timeout) {
    final List<Integer> lockedFiles = new ArrayList<>(orderedModifiedFiles.size());
    for (Integer fileId : orderedModifiedFiles) {
      if (tryLockFile(fileId, timeout))
        lockedFiles.add(fileId);
      else
        break;
    }

    if (lockedFiles.size() == orderedModifiedFiles.size()) {
      // OK: ALL LOCKED
      LogManager.instance().debug(this, "Locked files %s (threadId=%d)", orderedModifiedFiles, Thread.currentThread().getId());
      return lockedFiles;
    }

    // ERROR: UNLOCK LOCKED FILES
    unlockFilesInOrder(lockedFiles);

    throw new TransactionException("Timeout on locking resource during commit");
  }

  public void unlockFilesInOrder(final List<Integer> lockedFiles) {
    for (Integer fileId : lockedFiles)
      unlockFile(fileId);

    LogManager.instance().debug(this, "Unlocked files %s (threadId=%d)", lockedFiles, Thread.currentThread().getId());
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.readCacheRAM = totalReadCacheRAM.get();
    stats.writeCacheRAM = totalWriteCacheRAM.get();
    stats.pagesRead = totalPagesRead.get();
    stats.pagesReadSize = totalPagesReadSize.get();
    stats.pagesWritten = totalPagesWritten.get();
    stats.pagesWrittenSize = totalPagesWrittenSize.get();
    stats.pageFlushQueueLength = flushThread.queue.size();
    stats.cacheHits = cacheHits.get();
    stats.cacheMiss = cacheMiss.get();
    stats.concurrentModificationExceptions = totalConcurrentModificationExceptions.get();
    return stats;
  }

  private void putPageInCache(final ImmutablePage page) {
    if (readCache.put(page.pageId, page) == null)
      totalReadCacheRAM.addAndGet(page.getPhysicalSize());

    if (System.currentTimeMillis() - lastCheckForRAM > 500) {
      checkForPageDisposal();
      lastCheckForRAM = System.currentTimeMillis();
    }
  }

  private void removePageFromCache(final PageId pageId) {
    final ImmutablePage page = readCache.remove(pageId);
    if (page != null)
      totalReadCacheRAM.addAndGet(-1 * page.getPhysicalSize());
  }

  public boolean tryLockFile(final Integer fileId, final long timeout) {
    return lockManager.tryLock(fileId, Thread.currentThread(), timeout);
  }

  public void unlockFile(final Integer fileId) {
    lockManager.unlock(fileId, Thread.currentThread());
  }

  public void preloadFile(final int fileId) {
    LogManager.instance().debug(this, "Pre-loading file %d (threadId=%d)...", fileId, Thread.currentThread().getId());

    try {
      final PaginatedFile file = fileManager.getFile(fileId);
      final int pageSize = file.getPageSize();
      final int pages = (int) (file.getSize() / pageSize);

      for (int pageNumber = 0; pageNumber < pages; ++pageNumber)
        loadPage(new PageId(fileId, pageNumber), pageSize);

    } catch (IOException e) {
      throw new DatabaseMetadataException("Cannot load file in RAM", e);
    }
  }

  protected void flushPage(final ModifiablePage page) throws IOException {
    final PaginatedFile file = fileManager.getFile(page.pageId.getFileId());
    if (!file.isOpen())
      throw new DatabaseMetadataException("Cannot flush pages on disk because file is closed");

    LogManager.instance().debug(this, "Flushing page %s (threadId=%d)...", page, Thread.currentThread().getId());

    if (!flushOnlyAtClose) {
      putPageInCache(page.createImmutableView());

      final int written = file.write(page);

      // DELETE ONLY CURRENT VERSION OF THE PAGE (THIS PREVENT TO REMOVE NEWER PAGES)
      if (writeCache.remove(page.pageId, page))
        totalWriteCacheRAM.addAndGet(-1 * page.getPhysicalSize());

      totalPagesWritten.incrementAndGet();
      totalPagesWrittenSize.addAndGet(written);

      txManager.notifyPageFlushed(page);
    }
  }

  private ImmutablePage loadPage(final PageId pageId, final int size) throws IOException {
    if (System.currentTimeMillis() - lastCheckForRAM > 500) {
      checkForPageDisposal();
      lastCheckForRAM = System.currentTimeMillis();
    }

    final ImmutablePage page = new ImmutablePage(this, pageId, size);

    final PaginatedFile file = fileManager.getFile(pageId.getFileId());
    file.read(page);

    page.loadMetadata();

    LogManager.instance().debug(this, "Loaded page %s (threadId=%d)", page, Thread.currentThread().getId());

    totalPagesRead.incrementAndGet();
    totalPagesReadSize.addAndGet(page.getPhysicalSize());

    putPageInCache(page);

    return page;
  }

  private synchronized void checkForPageDisposal() {
    final long totalRAM = totalReadCacheRAM.get();

    if (totalRAM < maxRAM)
      return;

    final long ramToFree = maxRAM * GlobalConfiguration.FREE_PAGE_RAM.getValueAsInteger() / 100;

    LogManager.instance().debug(this, "Freeing RAM (target=%d, current %d > %d max threadId=%d)", ramToFree, totalRAM, maxRAM,
        Thread.currentThread().getId());

    // GET THE <DISPOSE_PAGES_PER_CYCLE> OLDEST PAGES
    long oldestPagesRAM = 0;
    final TreeSet<BasePage> oldestPages = new TreeSet<BasePage>(new Comparator<BasePage>() {
      @Override
      public int compare(final BasePage o1, final BasePage o2) {
        final int lastAccessed = Long.compare(o1.getLastAccessed(), o2.getLastAccessed());
        if (lastAccessed != 0)
          return lastAccessed;

        final int pageSize = Long.compare(o1.getPhysicalSize(), o2.getPhysicalSize());
        if (pageSize != 0)
          return pageSize;

        return o1.getPageId().compareTo(o2.getPageId());
      }
    });

    for (ImmutablePage page : readCache.values()) {
      if (oldestPagesRAM < ramToFree) {
        // FILL FIRST PAGES
        oldestPages.add(page);
        oldestPagesRAM += page.getPhysicalSize();
      } else {
        if (page.getLastAccessed() < oldestPages.last().getLastAccessed()) {
          oldestPages.add(page);
          oldestPagesRAM += page.getPhysicalSize();

          // REMOVE THE LESS OLD
          final Iterator<BasePage> it = oldestPages.iterator();
          final BasePage pageToRemove = it.next();
          oldestPagesRAM -= pageToRemove.getPhysicalSize();
          it.remove();
        }
      }
    }

    // REMOVE OLDEST PAGES FROM RAM
    long freedRAM = 0;
    for (BasePage page : oldestPages) {
      if (page instanceof ImmutablePage) {

        final ImmutablePage removedPage = readCache.remove(page.pageId);
        if (removedPage != null) {
          freedRAM += page.getPhysicalSize();
          totalReadCacheRAM.addAndGet(-1 * page.getPhysicalSize());
        }
      }
    }

    final long newTotalRAM = totalReadCacheRAM.get();

    if (LogManager.instance().isDebugEnabled())
      LogManager.instance().debug(this, "Freed %d RAM (current %d - %d max threadId=%d)", freedRAM, newTotalRAM, maxRAM,
          Thread.currentThread().getId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().warn(this, "Cannot free pages in RAM (current %d > %d max threadId=%d)", newTotalRAM, maxRAM,
          Thread.currentThread().getId());
  }
}
