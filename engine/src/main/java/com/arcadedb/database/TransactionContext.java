/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
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
public class TransactionContext implements Transaction {
  protected     DatabaseInternal         database;
  private       Map<PageId, MutablePage> modifiedPages;
  private       Map<PageId, MutablePage> newPages;
  private final Map<Integer, Integer>    newPageCounters       = new HashMap<>();
  private final Map<RID, Record>         immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>         modifiedRecordsCache  = new HashMap<>(1024);
  private       boolean                  useWAL;
  private       boolean                  asyncFlush            = true;
  private       WALFile.FLUSH_TYPE       walFlush;
  private       Collection<Integer>      lockedFiles;
  private final List<IndexKey>           indexKeysToLocks      = new ArrayList<>();
  private       long                     txId                  = -1;
  private       STATUS                   status                = STATUS.INACTIVE;

  public enum STATUS {INACTIVE, BEGUN, COMMIT_1ST_PHASE, COMMIT_2ND_PHASE}

  public static class IndexKey {
    public final boolean  add;
    public final Index    index;
    public final Object[] keyValues;
    public final RID      rid;

    public IndexKey(final boolean add, final Index index, final Object[] keyValues, final RID rid) {
      this.add = add;
      this.index = index;
      this.keyValues = keyValues;
      this.rid = rid;
    }

    @Override
    public String toString() {
      return "IndexKey(" + (add ? "add" : "del") + index.getName() + "=" + Arrays.toString(keyValues) + ")";
    }
  }

  public TransactionContext(final DatabaseInternal database) {
    this.database = database;
    this.walFlush = WALFile.getWALFlushType(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));
    this.useWAL = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
  }

  @Override
  public void begin() {
    if (status != STATUS.INACTIVE)
      throw new TransactionException("Transaction already begun");

    status = STATUS.BEGUN;

    modifiedPages = new HashMap<>();

    if (newPages == null)
      // KEEP ORDERING IN CASE MULTIPLE PAGES FOR THE SAME FILE ARE CREATED
      newPages = new LinkedHashMap<>();
  }

  @Override
  public Binary commit() {
    if (status == STATUS.INACTIVE)
      throw new TransactionException("Transaction not begun");

    if (status != STATUS.BEGUN)
      throw new TransactionException("Transaction already in commit phase");

    final Pair<Binary, List<MutablePage>> changes = commit1stPhase(true);

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

  @Override
  public void setUseWAL(final boolean useWAL) {
    this.useWAL = useWAL;
  }

  @Override
  public void setWALFlush(final WALFile.FLUSH_TYPE flush) {
    this.walFlush = flush;
  }

  @Override
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
  public MutablePage getPageToModify(final PageId pageId, final int size, final boolean isNew) throws IOException {
    if (!isActive())
      throw new TransactionException("Transaction not active");

    MutablePage page = modifiedPages.get(pageId);
    if (page == null) {
      if (newPages != null)
        page = newPages.get(pageId);

      if (page == null) {
        // NOT FOUND, DELEGATES TO THE DATABASE
        final BasePage loadedPage = database.getPageManager().getPage(pageId, size, isNew);
        if (loadedPage != null) {
          final MutablePage mutablePage = loadedPage.modify();
          if (isNew)
            newPages.put(pageId, mutablePage);
          else
            modifiedPages.put(pageId, mutablePage);
          page = mutablePage;
        }
      }
    }

    return page;
  }

  public MutablePage addPage(final PageId pageId, final int pageSize) {
    assureIsActive();

    // CREATE A PAGE ID BASED ON NEW PAGES IN TX. IN CASE OF ROLLBACK THEY ARE SIMPLY REMOVED AND THE GLOBAL PAGE COUNT IS UNCHANGED
    final MutablePage page = new MutablePage(database.getPageManager(), pageId, pageSize);
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

  @Override
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
   * Executes 1st phase from a replica.
   */
  public void commitFromReplica(final WALFile.WALTransaction buffer, final List<IndexKey> keysTx) throws TransactionException {
    try {
      for (WALFile.WALPage p : buffer.pages) {
        final PaginatedFile file = database.getFileManager().getFile(p.fileId);
        final int pageSize = file.getPageSize();

        assert !(database.getSchema().getFileById(p.fileId).getMainComponent() instanceof Index);

        final PageId pageId = new PageId(p.fileId, p.pageNumber);

        final boolean isNew = p.pageNumber >= file.getTotalPages();

        final MutablePage page = getPageToModify(pageId, pageSize, isNew);

        if (p.currentPageVersion != page.getVersion() + 1)
          throw new ConcurrentModificationException(
              "Concurrent modification on page " + page.getPageId() + " in file '" + file.getFileName() + "' (current v." + page.getVersion()
                  + " expected database v." + (page.getVersion() + 1) + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");

        // APPLY THE CHANGE TO THE PAGE
        page.writeByteArray(p.changesFrom - BasePage.PAGE_HEADER_SIZE, p.currentContent.content);
        page.setContentSize(p.currentPageSize);

        if (isNew) {
          newPages.put(pageId, page);
          newPageCounters.put(pageId.getFileId(), pageId.getPageNumber() + 1);
        } else
          modifiedPages.put(pageId, page);
      }

      indexKeysToLocks.addAll(keysTx);

    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      throw new TransactionException("Transaction error on commit", e);
    }

    database.commit();
  }

  /**
   * Locks the files in order, then checks all the pre-conditions.
   */
  public Pair<Binary, List<MutablePage>> commit1stPhase(final boolean isLeader) {
    if (status == STATUS.INACTIVE)
      throw new TransactionException("Transaction not started");

    if (status != STATUS.BEGUN)
      throw new TransactionException("Transaction already in commit phase");

    status = STATUS.COMMIT_1ST_PHASE;

    final int totalImpactedPages = modifiedPages.size() + (newPages != null ? newPages.size() : 0);
    if (totalImpactedPages == 0 && indexKeysToLocks.isEmpty()) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return null;
    }

    if (isLeader)
      // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
      lockedFiles = lockFilesInOrder();
    else
      // IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION
      lockedFiles = new ArrayList<>();

    try {
      if (isLeader && !indexKeysToLocks.isEmpty()) {
        // CHECK INDEX UNIQUE PUT (IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION)
        for (int i = 0; i < indexKeysToLocks.size(); ++i)
          applyIndexChangesAtCommit(indexKeysToLocks.get(i));
        indexKeysToLocks.clear();
      }

      // CHECK THE VERSIONS FIRST
      final List<MutablePage> pages = new ArrayList<>();

      final PageManager pageManager = database.getPageManager();

      for (final Iterator<MutablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
        final MutablePage p = it.next();

        final int[] range = p.getModifiedRange();
        if (range[1] > 0) {
          pageManager.checkPageVersion(p, false);
          pages.add(p);
        } else
          // PAGE NOT MODIFIED, REMOVE IT
          it.remove();
      }

      if (newPages != null)
        for (MutablePage p : newPages.values()) {
          final int[] range = p.getModifiedRange();
          if (range[1] > 0) {
            pageManager.checkPageVersion(p, true);
            pages.add(p);
          }
        }

      Binary result = null;

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

  public void commit2ndPhase(final Pair<Binary, List<MutablePage>> changes) {
    if (status != STATUS.COMMIT_1ST_PHASE)
      throw new TransactionException("Cannot execute 2nd phase commit without having started the 1st phase");

    status = STATUS.COMMIT_2ND_PHASE;

    final PageManager pageManager = database.getPageManager();

    try {
      if (changes.getFirst() != null)
        // WRITE TO THE WAL FIRST
        database.getTransactionManager().writeTransactionToWAL(changes.getSecond(), walFlush, txId, changes.getFirst());

      // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
      // UPDATE PAGE COUNTER FIRST
      LogManager.instance()
          .debug(this, "TX committing pages newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages, Thread.currentThread().getId());

      pageManager.updatePages(newPages, modifiedPages, asyncFlush);

      if (newPages != null) {
        for (Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
          database.getSchema().getFileById(entry.getKey()).setPageCount(entry.getValue());
          database.getFileManager().setVirtualFileSize(entry.getKey(), entry.getValue() * database.getFileManager().getFile(entry.getKey()).getPageSize());
        }
      }

      for (Record r : modifiedRecordsCache.values())
        ((RecordInternal) r).unsetDirty();

      for (int fileId : lockedFiles)
        database.getSchema().getFileById(fileId).onAfterCommit();

    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      LogManager.instance().info(this, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      reset();
    }
  }

  public void addIndexKeyLock(final IndexKey indexKey) {
    indexKeysToLocks.add(indexKey);
  }

  @Override
  public boolean isAsyncFlush() {
    return asyncFlush;
  }

  @Override
  public void setAsyncFlush(final boolean value) {
    this.asyncFlush = value;
  }

  public void reset() {
    status = STATUS.INACTIVE;

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

  public List<IndexKey> getIndexKeys() {
    return indexKeysToLocks;
  }

  public STATUS getStatus() {
    return status;
  }

  public void setStatus(final STATUS status) {
    this.status = status;
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void applyIndexChangesAtCommit(final IndexKey key) {
    if (key.add) {
      if (key.index.isUnique()) {
        final DocumentType type = database.getSchema().getType(key.index.getTypeName());

        // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
        final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(key.index.getPropertyNames());
        if (typeIndexes != null) {
          for (DocumentType.IndexMetadata i : typeIndexes) {
            final Set<RID> found = i.index.get(key.keyValues, 1);
            if (!found.isEmpty())
              throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(key.keyValues), found.iterator().next());
          }
        }
      }

      // AVOID CHECKING FOR UNIQUENESS BECAUSE IT HAS ALREADY BEEN CHECKED
      key.index.put(key.keyValues, key.rid);
    } else
      key.index.remove(key.keyValues, key.rid);
  }

  private List<Integer> lockFilesInOrder() {
    final Set<Integer> modifiedFiles = new HashSet<>();

    for (PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    for (IndexKey key : indexKeysToLocks) {
      modifiedFiles.add(key.index.getFileId());
      if (key.index.isUnique()) {
        // LOCK ALL THE FILES IMPACTED BY THE INDEX KEYS TO CHECK FOR UNIQUE CONSTRAINT
        final DocumentType type = database.getSchema().getType(key.index.getTypeName());
        final List<Bucket> buckets = type.getBuckets(false);
        for (Bucket b : buckets)
          modifiedFiles.add(b.getId());
      } else
        modifiedFiles.add(key.index.getAssociatedBucketId());
    }

    modifiedFiles.addAll(newPageCounters.keySet());

    final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    return database.getTransactionManager().tryLockFiles(modifiedFiles, timeout);
  }
}
