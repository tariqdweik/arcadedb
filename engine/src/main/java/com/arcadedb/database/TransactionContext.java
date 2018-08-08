/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.DocumentType;
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
  protected     DatabaseInternal              database;
  private       Map<PageId, ModifiablePage>   modifiedPages;
  private       Map<PageId, ModifiablePage>   newPages;
  private final Map<Integer, Integer>         newPageCounters       = new HashMap<>();
  private final Map<RID, Record>              immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>              modifiedRecordsCache  = new HashMap<>(1024);
  private       boolean                       useWAL                = GlobalConfiguration.TX_WAL.getValueAsBoolean();
  private       WALFile.FLUSH_TYPE            walFlush;
  private       List<Integer>                 lockedFiles;
  private final Set<DocumentIndexer.IndexKey> indexKeysToLocks      = new HashSet<>();
  private       long                          txId                  = -1;

  public TransactionContext(final DatabaseInternal database) {
    this.database = database;
    this.walFlush = WALFile.getWALFlushType(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));
  }

  public void begin() {
    if (modifiedPages != null)
      throw new TransactionException("Transaction already begun");

    modifiedPages = new HashMap<>();
  }

  public Binary commit() {
    if (modifiedPages == null)
      throw new TransactionException("Transaction not begun");

    final Pair<Binary, List<ModifiablePage>> changes = commit1stPhase();

    if (modifiedPages != null)
      commit2ndPhase(changes);
    else
      reset();

    return changes != null ? changes.getFirst() : null;
  }

  public Record getRecordFromCache(final RID rid) {
    Record rec = null;
    if (database.isReadYourWrites()) {
      rec = modifiedRecordsCache.get(rid);
      if (rec == null)
        rec = immutableRecordsCache.get(rid);
    }
    return rec;
  }

  public void updateRecordInCache(final Record record) {
    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot update record in TX cache because it is not persistent: " + record);

      if (record instanceof RecordInternal)
        modifiedRecordsCache.put(rid, record);
      else
        immutableRecordsCache.put(rid, record);
    }
  }

  public void removeImmutableRecordsOfSamePage(final RID rid) {
    final int bucketId = rid.getBucketId();
    final long pos = rid.getPosition();
    final long pageNum = pos / Bucket.MAX_RECORDS_IN_PAGE;

    // IMMUTABLE RECORD, AVOID IT'S POINTING TO THE OLD OFFSET IN A MODIFIED PAGE
    for (Iterator<Record> it = immutableRecordsCache.values().iterator(); it.hasNext(); ) {
      final Record r = it.next();

      if (r.getIdentity().getBucketId() == bucketId && r.getIdentity().getPosition() / Bucket.MAX_RECORDS_IN_PAGE == pageNum) {
        // SAME PAGE, REMOVE IT
        it.remove();
      }
    }
//
//    for (Iterator<Record> it = modifiedRecordsCache.values().iterator(); it.hasNext(); ) {
//      final BaseRecord r = (BaseRecord) it.next();
//      if (r.getIdentity().getBucketId() == bucketId && r.getIdentity().getPosition() / Bucket.MAX_RECORDS_IN_PAGE == pageNum) {
//        r.removeBuffer();
//      }
//    }
  }

  public void removeRecordFromCache(final Record record) {
    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot remove record in TX cache because it is not persistent: " + record);
      modifiedRecordsCache.remove(rid);
      immutableRecordsCache.remove(rid);
    }
  }

  public void setUseWAL(final boolean useWAL) {
    this.useWAL = useWAL;
  }

  public void setWALFlush(final WALFile.FLUSH_TYPE flush) {
    this.walFlush = flush;
  }

  public void rollback() {
    LogManager.instance()
        .debug(this, "Rollback transaction newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages, Thread.currentThread().getId());

    modifiedPages = null;
    newPages = null;

    // RELOAD PREVIOUS VERSION OF MODIFIED RECORDS
    if (database.isOpen())
      for (Record r : modifiedRecordsCache.values())
        r.reload();

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

    if (page == null) {
      // NOT FOUND, DELEGATES TO THE DATABASE
      page = database.getPageManager().getPage(pageId, size, false);
      if (page != null)
        page = page.createImmutableView();
    }

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
          final ModifiablePage modifiablePage = loadedPage.modify();
          modifiedPages.put(pageId, modifiablePage);
          page = modifiablePage;
        }
      }
    }

    return page;
  }

  public ModifiablePage addPage(final PageId pageId, final int pageSize) {
    assureIsActive();

    if (newPages == null)
      // KEEP ORDERING IN CASE MULTIPLE PAGES FOR THE SAME FILE ARE CREATED
      newPages = new LinkedHashMap<>();

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
    lockedFiles = null;
    modifiedPages = null;
    newPages = null;
    newPageCounters.clear();
  }

  /**
   * Locks the files in order, then checks all the pre-conditions.
   *
   * @return
   */
  public Pair<Binary, List<ModifiablePage>> commit1stPhase() {
    if (lockedFiles != null)
      throw new TransactionException("Cannot execute 1st phase commit because it was already started");

    final int totalImpactedPages = modifiedPages.size() + (newPages != null ? newPages.size() : 0);
    if (totalImpactedPages == 0) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return null;
    }

    Binary result = null;

    // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
    lockedFiles = lockFilesInOrder();
    try {
      // CHECK INDEX UNIQUE PUT
      for (DocumentIndexer.IndexKey lockedKey : indexKeysToLocks)
        database.getIndexer().postponeUniqueInsertion(lockedKey);

      // CHECK THE VERSIONS FIRST
      final List<ModifiablePage> pages = new ArrayList<>();

      final PageManager pageManager = database.getPageManager();

      for (final Iterator<ModifiablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
        final ModifiablePage p = it.next();

        final int[] range = p.getModifiedRange();
        if (range[1] > 0) {
          pageManager.checkPageVersion(p, false);
          pages.add(p);
        } else
          // PAGE NOT MODIFIED, REMOVE IT
          it.remove();
      }

      if (newPages != null)
        for (ModifiablePage p : newPages.values()) {
          final int[] range = p.getModifiedRange();
          if (range[1] > 0) {
            pageManager.checkPageVersion(p, true);
            pages.add(p);
          }
        }

      if (useWAL) {
        txId = database.getTransactionManager().getNextTransactionId();

        LogManager.instance().debug(this, "Creating buffer for TX %d (threadId=%d)", txId, Thread.currentThread().getId());

        result = database.getTransactionManager().createTransactionBuffer(txId, pages);
      }

      return new Pair(result, pages);

    } catch (DuplicatedKeyException e) {
      rollback();
      throw e;
    } catch (ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (Exception e) {
      LogManager.instance().info(this, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  public void commit2ndPhase(final Pair<Binary, List<ModifiablePage>> changes) {
    if (lockedFiles == null)
      throw new TransactionException("Cannot execute 2nd phase commit without having started the 1st phase");

    final PageManager pageManager = database.getPageManager();

    try {
      if (changes.getFirst() != null)
        // WRITE TO THE WAL FIRST
        database.getTransactionManager().writeTransactionToWAL(changes.getSecond(), walFlush, txId, changes.getFirst());

      // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
      // UPDATE PAGE COUNTER FIRST
      LogManager.instance()
          .debug(this, "TX committing pages newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages, Thread.currentThread().getId());

      pageManager.updatePages(newPages, modifiedPages);

      if (newPages != null) {
        for (Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
          database.getSchema().getFileById(entry.getKey()).setPageCount(entry.getValue());
          database.getFileManager().setVirtualFileSize(entry.getKey(), entry.getValue() * database.getFileManager().getFile(entry.getKey()).getPageSize());
        }
      }

      for (Record r : modifiedRecordsCache.values())
        ((RecordInternal) r).unsetDirty();

    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      LogManager.instance().info(this, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      reset();
    }
  }

  private List<Integer> lockFilesInOrder() {
    final Set<Integer> modifiedFiles = new HashSet<>();

    for (PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    // LOCK ALL THE FILE IMPACTED BY THE INDEX KEYS
    for (DocumentIndexer.IndexKey key : indexKeysToLocks) {
      final DocumentType type = database.getSchema().getType(key.typeName);
      final List<Bucket> buckets = type.getBuckets(false);
      for (Bucket b : buckets)
        modifiedFiles.add(b.getId());
    }

    final long timeout = GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValueAsLong();

    return database.getTransactionManager().tryLockFiles(modifiedFiles, timeout);
  }

  private void reset() {
    final TransactionManager txManager = database.getTransactionManager();

    if (lockedFiles != null) {
      txManager.unlockFilesInOrder(lockedFiles);
      lockedFiles = null;
    }

    indexKeysToLocks.clear();

    modifiedPages = null;
    newPages = null;
    newPageCounters.clear();
    modifiedRecordsCache.clear();
    immutableRecordsCache.clear();
    txId = -1;
  }

  public void addIndexKeyLock(final DocumentIndexer.IndexKey indexKey) {
    indexKeysToLocks.add(indexKey);
  }
}
