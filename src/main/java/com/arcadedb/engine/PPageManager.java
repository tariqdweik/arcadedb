package com.arcadedb.engine;

import com.arcadedb.PGlobalConfiguration;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PConfigurationException;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.utility.PLockContext;
import com.arcadedb.utility.PLogManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages pages from disk to RAM. Each page can have different size. TODO: check if the lock on flush is necessary
 */
public class PPageManager extends PLockContext {
  private final PFileManager fileManager;
  private final ConcurrentMap<PPageId, PImmutablePage>  pageMap        = new ConcurrentHashMap<PPageId, PImmutablePage>(65536);
  private final ConcurrentMap<PPageId, PModifiablePage> modifiedPages  = new ConcurrentHashMap<PPageId, PModifiablePage>(65536);
  private final Set<PPageId>                            pagesToDispose = new HashSet<PPageId>();
  private long maxRAM;
  private AtomicLong totalImmutablePagesRAM = new AtomicLong();
  private AtomicLong totalModifiedPagesRAM  = new AtomicLong();
  private AtomicLong totalPagesRead         = new AtomicLong();
  private AtomicLong totalPagesReadSize     = new AtomicLong();
  private AtomicLong totalPagesWritten      = new AtomicLong();
  private AtomicLong totalPagesWrittenSize  = new AtomicLong();

  private long lastCheckForRAM = 0;
  private final PPageManagerFlushThread flushThread;

  public class PPageManagerStats {
    public long maxRAM;
    public long totalImmutablePagesRAM;
    public long totalModifiedPagesRAM;
    public long pagesRead;
    public long pagesReadSize;
    public long pagesWrittenSize;
    public long pagesWritten;
    public int  pagesToDispose;
  }

  public PPageManager(final PFileManager fileManager) {
    super(true);

    this.fileManager = fileManager;
    maxRAM = PGlobalConfiguration.MAX_PAGE_RAM.getValueAsLong() * 1024;
    if (maxRAM < 0)
      throw new PConfigurationException(PGlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + maxRAM + ")");
    flushThread = new PPageManagerFlushThread(this);
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

    try {
      flushNow();
    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on flushing modified pages to disk", e);
    }

    pageMap.clear();
    totalImmutablePagesRAM.set(0);
  }

  public void disposeFile(final int fileId) throws IOException {
    for (Iterator<PModifiablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
      final PModifiablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalModifiedPagesRAM.addAndGet(-1 * p.getPhysicalSize());
        it.remove();
      }
    }
    for (Iterator<PImmutablePage> it = pageMap.values().iterator(); it.hasNext(); ) {
      final PImmutablePage p = it.next();
      if (p.getPageId().getFileId() == fileId) {
        totalImmutablePagesRAM.addAndGet(-1 * p.getPhysicalSize());
        it.remove();
      }
    }
  }

  public void flushFile(final int fileId) throws IOException {
    for (Iterator<PModifiablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
      final PModifiablePage p = it.next();
      if (p.getPageId().getFileId() == fileId)
        flushPage(p);
    }
  }

  public PBasePage getPage(final PPageId pageId, final int size) throws IOException {
    final PModifiablePage modifiedPage = modifiedPages.get(pageId);
    if (modifiedPage != null)
      return modifiedPage;

    PImmutablePage page = pageMap.get(pageId);
    if (page == null)
      page = loadPage(pageId, size);
    else
      page.updateLastAccesses();

    if (page == null)
      throw new IllegalArgumentException("Page id '" + pageId + "' does not exists");
    return page;
  }

  public PModifiablePage getPageToModify(final PPageId pageId, final int size) throws IOException {
    return getPage(pageId, size).createModifiableCopy();
  }

  public void checkPageVersion(final PModifiablePage page) throws IOException {
    final PBasePage p = getPage(page.getPageId(), page.getPhysicalSize());
    if (p != null && p.getVersion() != page.getVersion())
      throw new PConcurrentModificationException(
          "Concurrent modification on page " + page.getPageId() + " (current v." + page.getVersion() + " <> database v." + p
              .getVersion() + "). Please retry the operation");
  }

  public void updatePage(final PModifiablePage page) throws IOException {
    final PBasePage p = getPage(page.getPageId(), page.getPhysicalSize());
    if (p != null) {
      if (p.getVersion() != page.getVersion())
        throw new PConcurrentModificationException(
            "Concurrent modification on page " + page.getPageId() + " (current v." + page.getVersion() + " <> database v." + p
                .getVersion() + "). Please retry the operation");

      page.incrementVersion();
      page.flushMetadata();

      if (modifiedPages.put(page.pageId, page) == null)
        totalModifiedPagesRAM.addAndGet(page.getPhysicalSize());
      if (pageMap.remove(page.getPageId()) != null)
        totalImmutablePagesRAM.addAndGet(page.getPhysicalSize() * -1);
    }
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.totalImmutablePagesRAM = totalImmutablePagesRAM.get();
    stats.totalModifiedPagesRAM = totalModifiedPagesRAM.get();
    stats.pagesRead = totalPagesRead.get();
    stats.pagesReadSize = totalPagesReadSize.get();
    stats.pagesWritten = totalPagesWritten.get();
    stats.pagesWrittenSize = totalPagesWrittenSize.get();
    stats.pagesToDispose = pagesToDispose.size();
    return stats;
  }

  public void addPagesToDispose(final Collection<PPageId> pageIds) {
    synchronized (pagesToDispose) {
      pagesToDispose.addAll(pageIds);
      removeCandidatePages();
    }
  }

  protected void flushPage(final PModifiablePage page) throws IOException {
    executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final PFile file = fileManager.getFile(page.getPageId().getFileId());
        if (!file.isOpen())
          throw new PDatabaseMetadataException("Cannot flush pages on disk because file is closed");

        file.write(page);
        totalPagesWritten.incrementAndGet();
        totalPagesWrittenSize.addAndGet(page.getPhysicalSize());
        return null;
      }
    });
  }

  private PImmutablePage loadPage(final PPageId pageId, final int size) throws IOException {
    if (System.currentTimeMillis() - lastCheckForRAM > 10) {
      checkForPageDisposal();
      lastCheckForRAM = System.currentTimeMillis();
    }

    final PImmutablePage page = new PImmutablePage(this, pageId, size);

    final PFile file = fileManager.getFile(pageId.getFileId());
    file.read(page);
    page.loadMetadata();

    totalPagesRead.incrementAndGet();
    totalPagesReadSize.addAndGet(page.getPhysicalSize());

    if (pageMap.put(pageId, page) == null)
      totalImmutablePagesRAM.addAndGet(page.getPhysicalSize());

    return page;
  }

  private void checkForPageDisposal() {
    final long totalRAM = totalImmutablePagesRAM.get() + totalModifiedPagesRAM.get();

    if (totalRAM < maxRAM)
      return;

    final long ramToFree = maxRAM * PGlobalConfiguration.FREE_PAGE_RAM.getValueAsInteger() / 100;

//    if (PLogManager.instance().isDebugEnabled())
    PLogManager.instance()
        .info(this, "Freeing RAM (target=%d, current %d > %d max, modifiedPagesRAM=%d)", ramToFree, totalRAM, maxRAM,
            totalModifiedPagesRAM.get());

    // GET THE <DISPOSE_PAGES_PER_CYCLE> OLDEST PAGES
    long oldestPagesRAM = 0;
    final TreeSet<PBasePage> oldestPages = new TreeSet<PBasePage>(new Comparator<PBasePage>() {
      @Override
      public int compare(final PBasePage o1, final PBasePage o2) {
        final int lastAccessed = Long.compare(o1.getLastAccessed(), o2.getLastAccessed());
        if (lastAccessed != 0)
          return lastAccessed;

        final int pageSize = Long.compare(o1.getPhysicalSize(), o2.getPhysicalSize());
        if (pageSize != 0)
          return pageSize;

        return o1.getPageId().compareTo(o2.getPageId());
      }
    });

    for (PImmutablePage page : pageMap.values()) {
      if (oldestPagesRAM < ramToFree) {
        // FILL FIRST PAGES
        oldestPages.add(page);
        oldestPagesRAM += page.getPhysicalSize();
      } else {
        if (page.getLastAccessed() < oldestPages.last().getLastAccessed()) {
          oldestPages.add(page);
          oldestPagesRAM += page.getPhysicalSize();

          // REMOVE THE LESS OLD
          final Iterator<PBasePage> it = oldestPages.iterator();
          final PBasePage pageToRemove = it.next();
          oldestPagesRAM -= pageToRemove.getPhysicalSize();
          it.remove();
        }
      }
    }

    for (PModifiablePage page : modifiedPages.values()) {
      if (oldestPagesRAM < ramToFree) {
        // FILL FIRST PAGES
        oldestPages.add(page);
        oldestPagesRAM += page.getPhysicalSize();
      } else {
        final PBasePage lastOldest = oldestPages.last();
        if (page.getLastAccessed() < lastOldest.getLastAccessed()) {
          oldestPages.add(page);
          oldestPagesRAM += page.getPhysicalSize();

          // REMOVE THE LESS OLD
          oldestPages.remove(lastOldest);
          oldestPagesRAM -= lastOldest.getPhysicalSize();
        }
      }
    }

    // REMOVE OLDEST PAGES FROM RAM
    long freedRAM = 0;
    for (PBasePage page : oldestPages) {
      if (page instanceof PImmutablePage) {

        final PImmutablePage removedPage = pageMap.remove(page.pageId);
        if (removedPage != null) {
          freedRAM += page.getPhysicalSize();
          totalImmutablePagesRAM.addAndGet(-1 * page.getPhysicalSize());
        }
      } else {
        try {
          flushThread.asyncFlush((PModifiablePage) page);
        } catch (Exception e) {
          PLogManager.instance().error(this, "Error on flushing page", e);
        }

        final boolean removed = modifiedPages.remove(page.pageId) != null;
        if (removed) {
          freedRAM += page.getPhysicalSize();
          totalModifiedPagesRAM.addAndGet(-1 * page.getPhysicalSize());
        }
      }
    }

    final long newTotalRAM = totalImmutablePagesRAM.get() + totalModifiedPagesRAM.get();

//    if (PLogManager.instance().isDebugEnabled())
    PLogManager.instance().info(this, "Freed %d RAM (current %d > %d max, modifiedPagesRAM=%d)", freedRAM, newTotalRAM, maxRAM,
        totalModifiedPagesRAM.get());

    if (newTotalRAM > maxRAM) {
      PLogManager.instance().warn(this, "Cannot free pages in RAM (current %d > %d max, modifiedPagesRAM=%d)", newTotalRAM, maxRAM,
          totalModifiedPagesRAM.get());
    }
  }

  public void removeCandidatePages() {
    int disposedPages = 0;
    long freedRAM = 0;

    synchronized (pagesToDispose) {
      for (PPageId pageId : pagesToDispose) {
        PBasePage page = modifiedPages.remove(pageId);
        if (page != null) {
          try {
            flushThread.asyncFlush((PModifiablePage) page);
            ++disposedPages;
            freedRAM += page.getPhysicalSize();
            totalModifiedPagesRAM.addAndGet(-1 * page.getPhysicalSize());
          } catch (Exception e) {
            PLogManager.instance().error(this, "Error on flushing page", e);
          }
        }

        page = pageMap.remove(pageId);
        if (page != null) {
          ++disposedPages;
          freedRAM += page.getPhysicalSize();
          totalImmutablePagesRAM.addAndGet(-1 * page.getPhysicalSize());
        }
      }
      pagesToDispose.clear();
    }

    if (disposedPages > 0) {
      if (PLogManager.instance().isDebugEnabled())
        PLogManager.instance().debug(this, "Disposed %d pages (totalDisposedRAM=%d)", disposedPages, freedRAM);
    }
  }

  private void flushNow() throws IOException {
    executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        for (PModifiablePage p : modifiedPages.values())
          try {
            flushPage(p);
          } catch (IOException e) {
            throw new PDatabaseOperationException("Error on flushing page " + p.getPageId() + " to disk", e);
          }
        modifiedPages.clear();
        totalModifiedPagesRAM.set(0);
        return null;
      }
    });
  }
}
