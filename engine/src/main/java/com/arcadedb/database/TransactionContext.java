package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ModifiablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.LogManager;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Manage the transaction context. When the transaction begins, the modifiedPages map is initialized. This allows to always delegate
 * to the transaction context, even if there is not active transaction by ignoring tx data. This keeps code smaller.
 * <p>
 * At commit time, the files are locked in order (to avoid deadlocks) and to allow parallel commit on different files.
 * <p>
 * Format of WAL:
 * <p>
 * txId:long|pages:int|&lt;segmentSize:int|fileId:int|pageNumber:long|pageModifiedFrom:int|pageModifiedTo:int|&lt;prevContent&gt;&lt;newContent&gt;segmentSize:int&gt;MagicNumber:long
 */
public class TransactionContext {
  private final DatabaseInternal            database;
  private       Map<PageId, ModifiablePage> modifiedPages;
  private       Map<PageId, ModifiablePage> newPages;
  private final Map<Integer, Integer>       newPageCounters = new HashMap<>();
  private final Map<RID, Record>            cache           = new HashMap<>(1024);
  private       boolean                     useWAL          = GlobalConfiguration.TX_WAL.getValueAsBoolean();
  private       boolean                     sync            = GlobalConfiguration.TX_FLUSH.getValueAsBoolean();

  public TransactionContext(final DatabaseInternal database) {
    this.database = database;
  }

  public void begin() {
    if (modifiedPages != null)
      throw new TransactionException("Transaction already begun");

    modifiedPages = new HashMap<>();
  }

  public void commit() {
    if (modifiedPages == null)
      throw new TransactionException("Transaction not begun");

    final int totalImpactedPages = modifiedPages.size() + (newPages != null ? newPages.size() : 0);
    if (totalImpactedPages == 0) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return;
    }

    final PageManager pageManager = database.getPageManager();

    // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
    final List<Integer> lockedFiles = lockFilesInOrder(pageManager);
    try {

      // CHECK THE VERSION FIRST
      final List<Pair<BasePage, ModifiablePage>> pages = new ArrayList<>();

      for (final Iterator<ModifiablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
        final ModifiablePage p = it.next();

        final int[] range = p.getModifiedRange();
        if (range[1] > 0)
          pages.add(new Pair<>(pageManager.checkPageVersion(p, false), p));
        else
          // PAGE NOT MODIFIED, REMOVE IT
          it.remove();
      }

      if (newPages != null)
        for (ModifiablePage p : newPages.values()) {
          final int[] range = p.getModifiedRange();
          if (range[1] > 0) {
            pageManager.checkPageVersion(p, true);
            pages.add(new Pair<>(null, p));
          }
        }

      if (useWAL)
        database.getTransactionManager().writeTransactionToWAL(pages, sync);

      try {
        // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
        // UPDATE PAGE COUNTER FIRST
        if (newPages != null) {
          for (Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
            database.getSchema().getFileById(entry.getKey()).setPageCount(entry.getValue());
            database.getFileManager().setVirtualFileSize(entry.getKey(),
                entry.getValue() * database.getFileManager().getFile(entry.getKey()).getPageSize());
          }
        }

        LogManager.instance().debug(this, "Committing pages newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
            Thread.currentThread().getId());

        pageManager.updatePages(newPages, modifiedPages);

      } catch (Exception e) {
        throw new TransactionException("Unexpected transaction error. Unable to recover the transaction", e);
      }

    } catch (ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (Exception e) {
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      pageManager.unlockFilesInOrder(lockedFiles);
    }

    reset();
  }

  public Record getRecordFromCache(final RID rid) {
    if (database.isReadYourWrites())
      return cache.get(rid);
    return null;
  }

  public void updateRecordInCache(final Record record) {
    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot update record in TX cache because it is not persistent: " + record);
      cache.put(rid, record);
    }
  }

  public void removeRecordFromCache(final Record record) {
    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot remove record in TX cache because it is not persistent: " + record);
      cache.remove(rid);
    }
  }

  public boolean isUseWAL() {
    return useWAL;
  }

  public void setUseWAL(final boolean useWAL) {
    this.useWAL = useWAL;
  }

  public boolean isSync() {
    return sync;
  }

  public void setSync(final boolean sync) {
    this.sync = sync;
  }

  public void rollback() {
    LogManager.instance().debug(this, "Rollback transaction newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
        Thread.currentThread().getId());

    reset();
  }

  public void assureIsActive() {
    if (modifiedPages == null)
      throw new TransactionException("Transaction not begun");
  }

  /**
   * Looks for the page in the TX context first, then delegates to the database.
   */
  public BasePage getPage(final PageId pageId, final int size) throws IOException {
    BasePage page = null;

    if (modifiedPages != null)
      page = modifiedPages.get(pageId);

    if (page == null && newPages != null)
      page = newPages.get(pageId);

    if (page == null)
      // NOT FOUND, DELEGATES TO THE DATABASE
      page = database.getPageManager().getPage(pageId, size, false);

    return page;
  }

  /**
   * If the page is not already in transaction tx, loads from the database and clone it locally.
   */
  public ModifiablePage getPageToModify(final PageId pageId, final int size, final boolean isNew) throws IOException {
    if (!isActive())
      throw new TransactionException("Transaction not active");

    ModifiablePage page = modifiedPages.get(pageId);
    if (page == null) {
      if (newPages != null)
        page = newPages.get(pageId);

      if (page == null) {
        // NOT FOUND, DELEGATES TO THE DATABASE
        final BasePage loadedPage = database.getPageManager().getPage(pageId, size, isNew);
        if (loadedPage != null) {
          ModifiablePage modifiablePage = loadedPage.modify();
          modifiedPages.put(pageId, modifiablePage);
          page = modifiablePage;
        }
      }
    }

    return page;
  }

  public void removeModifiedPage(final PageId pageId) {
    modifiedPages.remove(pageId);
  }

  public ModifiablePage addPage(final PageId pageId, final int pageSize) {
    assureIsActive();

    if (newPages == null)
      newPages = new HashMap<>();

    // CREATE A PAGE ID BASED ON NEW PAGES IN TX. IN CASE OF ROLLBACK THEY ARE SIMPLY REMOVED AND THE GLOBAL PAGE COUNT IS UNCHANGED
    final ModifiablePage page = new ModifiablePage(database.getPageManager(), pageId, pageSize);
    newPages.put(pageId, page);

    final Integer indexCounter = newPageCounters.get(pageId.getFileId());
    if (indexCounter == null || indexCounter < pageId.getPageNumber() + 1)
      newPageCounters.put(pageId.getFileId(), pageId.getPageNumber() + 1);

    return page;
  }

  public long getFileSize(final int fileId) throws IOException {
    final Integer lastPage = newPageCounters.get(fileId);
    if (lastPage != null)
      return (lastPage + 1) * database.getFileManager().getFile(fileId).getPageSize();

    return database.getFileManager().getVirtualFileSize(fileId);
  }

  public Integer getPageCounter(final int indexFileId) {
    return newPageCounters.get(indexFileId);
  }

  public boolean isActive() {
    return modifiedPages != null;
  }

  public Map<String, Object> stats() {
    final Map<String, Object> map = new HashMap<>();

    final Set<Integer> involvedFiles = new LinkedHashSet<>();
    for (PageId pid : modifiedPages.keySet())
      involvedFiles.add(pid.getFileId());
    for (PageId pid : newPages.keySet())
      involvedFiles.add(pid.getFileId());
    for (Integer fid : newPageCounters.keySet())
      involvedFiles.add(fid);

    map.put("involvedFiles", involvedFiles);

    map.put("modifiedPages", modifiedPages.size());
    map.put("newPages", newPages != null ? newPages.size() : 0);
    map.put("newPageCounters", newPageCounters);
    return map;
  }

  public int getModifiedPages() {
    int result = 0;
    if (modifiedPages != null)
      result += modifiedPages.size();
    if (newPages != null)
      result += newPages.size();
    return result;
  }

  /**
   * Test only API.
   */
  public void kill() {
    modifiedPages = null;
    newPages = null;
    newPages = null;
    newPageCounters.clear();
  }

  private List<Integer> lockFilesInOrder(final PageManager pageManager) {
    final Set<Integer> modifiedFiles = new HashSet<>();
    for (PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    final List<Integer> orderedModifiedFiles = new ArrayList<>(modifiedFiles);
    Collections.sort(orderedModifiedFiles);

    final long timeout = GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValueAsLong();

    return pageManager.tryLockFiles(orderedModifiedFiles, timeout);
  }

  private void reset() {
    modifiedPages = null;
    newPages = null;
    newPageCounters.clear();
    cache.clear();
  }
}
