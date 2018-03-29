package com.arcadedb.database;

import com.arcadedb.PGlobalConfiguration;
import com.arcadedb.engine.PBasePage;
import com.arcadedb.engine.PModifiablePage;
import com.arcadedb.engine.PPageId;
import com.arcadedb.engine.PPageManager;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PTransactionException;
import com.arcadedb.utility.PPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Manage the transaction context. When the transaction begins, the modifiedPages map is initialized. This allows to always delegate
 * to the transaction context, even if there is not active transaction by ignoring tx data. This keeps code smaller.
 * <p>
 * At commit time, the files are locked in order (to avoid deadlocks) and to allow parallel commit on different files.
 */
public class PTransactionContext {
  private static final int  TXLOGSEGMENT_HEADER_SIZE =
      PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE
          + PBinary.LONG_SERIALIZED_SIZE;
  private static final int  TXLOGSEGMENT_FOOTER_SIZE = PBinary.INT_SERIALIZED_SIZE + PBinary.LONG_SERIALIZED_SIZE;
  private static final long MAGIC_NUMBER             = 9371515385058702l;
  private final PDatabaseInternal             database;
  private       Map<PPageId, PModifiablePage> modifiedPages;
  private       Map<PPageId, PModifiablePage> newPages;
  private final Map<Integer, Integer> newPageCounters = new HashMap<>();
  private       boolean               useWAL          = true;
  private       boolean               sync            = false;

  public PTransactionContext(final PDatabaseInternal database) {
    this.database = database;
  }

  public void begin() {
    if (modifiedPages != null)
      throw new PTransactionException("Transaction already begun");

    modifiedPages = new HashMap<>();
  }

  public void commit() {
    if (modifiedPages == null)
      throw new PTransactionException("Transaction not begun");

    final PPageManager pageManager = database.getPageManager();

    // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
    final List<Integer> lockedFiles = lockFilesInOrder(pageManager);
    try {

      // CHECK THE VERSION FIRST
      final List<PPair<PBasePage, PModifiablePage>> pages = new ArrayList<>();
      for (PModifiablePage p : modifiedPages.values())
        pages.add(new PPair<>(pageManager.checkPageVersion(p, false), p));

      if (newPages != null)
        for (PModifiablePage p : newPages.values()) {
          pageManager.checkPageVersion(p, true);
          pages.add(new PPair<>(null, p));
        }

      appendWAL(pages);

      // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
      for (PModifiablePage p : modifiedPages.values())
        pageManager.updatePage(p, false);

      if (newPages != null) {
        for (PModifiablePage p : newPages.values())
          pageManager.updatePage(p, true);

        for (Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
          database.getSchema().getFileById(entry.getKey()).onAfterCommit(entry.getValue());
          database.getFileManager().setVirtualFileSize(entry.getKey(),
              entry.getValue() * database.getFileManager().getFile(entry.getKey()).getPageSize());
        }
      }

    } catch (PConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (Exception e) {
      rollback();
      throw new PTransactionException("Transaction error on commit", e);
    } finally {
      unlockFilesInOrder(pageManager, lockedFiles);
    }

    reset();
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
    reset();
  }

  public void assureIsActive() {
    if (modifiedPages == null)
      throw new PTransactionException("Transaction not begun");
  }

  /**
   * Looks for the page in the TX context first, then delegates to the database.
   */
  public PBasePage getPage(final PPageId pageId, final int size) throws IOException {
    PBasePage page = null;

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
  public PModifiablePage getPageToModify(final PPageId pageId, final int size, final boolean isNew) throws IOException {
    if (!isActive())
      throw new PTransactionException("Transaction not active");

    PModifiablePage page = modifiedPages.get(pageId);
    if (page == null) {
      if (newPages != null)
        page = newPages.get(pageId);

      if (page == null) {
        // NOT FOUND, DELEGATES TO THE DATABASE
        final PBasePage loadedPage = database.getPageManager().getPage(pageId, size, isNew);
        if (loadedPage != null) {
          PModifiablePage modifiablePage = loadedPage.modify();
          modifiedPages.put(pageId, modifiablePage);
          page = modifiablePage;
        }
      }
    }

    return page;
  }

  public void removeModifiedPage(final PPageId pageId) {
    modifiedPages.remove(pageId);
  }

  public PModifiablePage addPage(final PPageId pageId, final int pageSize) {
    assureIsActive();

    if (newPages == null)
      newPages = new HashMap<>();

    // CREATE A PAGE ID BASED ON NEW PAGES IN TX. IN CASE OF ROLLBACK THEY ARE SIMPLY REMOVED AND THE GLOBAL PAGE COUNT IS UNCHANGED
    final PModifiablePage page = new PModifiablePage(database.getPageManager(), pageId, pageSize);
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
    for (PPageId pid : modifiedPages.keySet())
      involvedFiles.add(pid.getFileId());
    for (PPageId pid : newPages.keySet())
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

  private List<Integer> lockFilesInOrder(final PPageManager pageManager) {
    final Set<Integer> modifiedFiles = new HashSet<>();
    for (PPageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (PPageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    final List<Integer> orderedModifiedFiles = new ArrayList<>(modifiedFiles);
    Collections.sort(orderedModifiedFiles);

    final long timeout = PGlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValueAsLong();

    final List<Integer> lockedFiles = new ArrayList<>(orderedModifiedFiles.size());

    for (Integer fileId : orderedModifiedFiles) {
      if (pageManager.tryLockFile(fileId, timeout))
        lockedFiles.add(fileId);
      else
        break;
    }

    if (lockedFiles.size() == orderedModifiedFiles.size())
      // OK: ALL LOCKED
      return lockedFiles;

    // ERROR: UNLOCK LOCKED FILES
    unlockFilesInOrder(pageManager, lockedFiles);

    throw new PTransactionException("Timeout on locking resource during commit");
  }

  private void unlockFilesInOrder(final PPageManager pageManager, final List<Integer> lockedFiles) {
    for (Integer fileId : lockedFiles)
      pageManager.unlockFile(fileId);
  }

  private void reset() {
    modifiedPages = null;
    newPages = null;
    newPageCounters.clear();
  }

  private void appendWAL(final List<PPair<PBasePage, PModifiablePage>> pages) throws IOException {
    if (!useWAL)
      return;

    for (PPair<PBasePage, PModifiablePage> entry : pages) {
      final PBasePage prevPage = entry.getFirst();
      final PModifiablePage newPage = entry.getSecond();

      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      final int segmentSize = TXLOGSEGMENT_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2)) + TXLOGSEGMENT_FOOTER_SIZE;

      final byte[] buffer = new byte[segmentSize];
      final PBinary binary = new PBinary(buffer, segmentSize);

      binary.position(0);
      binary.putInt(segmentSize);
      binary.putInt(newPage.getPageId().getFileId());
      binary.putLong(newPage.getPageId().getPageNumber());
      binary.putInt(deltaRange[0]);
      binary.putInt(deltaRange[1]);
      if (prevPage != null) {
        final ByteBuffer prevPageBuffer = prevPage.getContent();
        prevPageBuffer.position(deltaRange[0]);
        prevPageBuffer.get(buffer, TXLOGSEGMENT_HEADER_SIZE, deltaSize);

        final ByteBuffer newPageBuffer = newPage.getContent();
        newPageBuffer.position(deltaRange[0]);
        newPageBuffer.get(buffer, TXLOGSEGMENT_HEADER_SIZE + deltaSize, deltaSize);
      } else {
        final ByteBuffer newPageBuffer = newPage.getContent();
        newPageBuffer.position(deltaRange[0]);
        newPageBuffer.get(buffer, TXLOGSEGMENT_HEADER_SIZE, deltaSize);
      }
      binary.position(TXLOGSEGMENT_HEADER_SIZE + (deltaSize * (prevPage == null ? 1 : 2)));
      binary.putInt(segmentSize);
      binary.putLong(MAGIC_NUMBER);

      database.getWALFile().append(binary.buffer, sync);
    }
  }
}
