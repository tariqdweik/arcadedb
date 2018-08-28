/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LSM-Tree index implementation. It relies on a mutable index and its underlying immutable, compacted index.
 */
public class LSMTreeIndex implements Index {
  private   LSMTreeIndexMutable                                     mutable;
  private   RWLockContext                                           lock             = new RWLockContext();
  protected AtomicReference<LSMTreeIndexAbstract.COMPACTING_STATUS> compactingStatus = new AtomicReference<>(LSMTreeIndexAbstract.COMPACTING_STATUS.NO);

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize) throws IOException {
      return new LSMTreeIndex(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      final LSMTreeIndex mainIndex = new LSMTreeIndex(database, name, true, filePath, id, mode, pageSize);
      return new LSMTreeIndexMutable(mainIndex, database, name, true, filePath, id, mode, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException {
      final LSMTreeIndex mainIndex = new LSMTreeIndex(database, name, false, filePath, id, mode, pageSize);
      return new LSMTreeIndexMutable(mainIndex, database, name, false, filePath, id, mode, pageSize);
    }
  }

  /**
   * Called at creation time.
   */
  public LSMTreeIndex(final Database database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode, final byte[] keyTypes,
      final int pageSize) throws IOException {
    mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, mode, keyTypes, pageSize);
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndex(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, id, mode, pageSize);
  }

  public boolean scheduleCompaction() {
    return compactingStatus.compareAndSet(LSMTreeIndexAbstract.COMPACTING_STATUS.NO, LSMTreeIndexAbstract.COMPACTING_STATUS.SCHEDULED);
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    if (mutable.getDatabase().getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update the index '" + getName() + "'");

    if (!compactingStatus.compareAndSet(LSMTreeIndexAbstract.COMPACTING_STATUS.SCHEDULED, LSMTreeIndexAbstract.COMPACTING_STATUS.IN_PROGRESS))
      // ALREADY COMPACTING
      return false;

    try {
      return LSMTreeIndexCompactor.compact(this);
    } finally {
      compactingStatus.set(LSMTreeIndexAbstract.COMPACTING_STATUS.NO);
    }
  }

  public LSMTreeIndexMutable copyPagesToNewFile(final int startingFromPage, final LSMTreeIndexCompacted subIndex) {
    final Database database = mutable.getDatabase();

    // COPY MUTABLE PAGES TO THE NEW FILE
    final LSMTreeIndexMutable newIndex = lock.executeInReadLock(() -> {
      final int pageSize = mutable.getPageSize();

      int last_ = mutable.getName().lastIndexOf('_');
      final String newName = mutable.getName().substring(0, last_) + "_" + System.currentTimeMillis();

      final LSMTreeIndexMutable newMutableIndex = new LSMTreeIndexMutable(this, database, newName, mutable.isUnique(),
          database.getDatabasePath() + "/" + newName, mutable.getKeyTypes(), pageSize, LSMTreeIndexAbstract.COMPACTING_STATUS.NO, subIndex);

      // KEEP METADATA AND LEAVE IT EMPTY
      newMutableIndex.createNewPage(0, false);

      for (int i = 0; i < mutable.getTotalPages() - startingFromPage; ++i) {
        final BasePage currentPage = database.getTransaction().getPage(new PageId(newMutableIndex.getFileId(), i + startingFromPage), pageSize);

        // COPY THE ENTIRE PAGE TO THE NEW INDEX
        final MutablePage newPage = newMutableIndex.createNewPage(0, false);

        final ByteBuffer pageContent = currentPage.getContent();
        pageContent.rewind();
        newPage.getContent().put(pageContent);
      }

      newMutableIndex.currentMutablePages = newMutableIndex.getTotalPages();

      return newMutableIndex;
    });

    // SWAP OLD WITH NEW INDEX IN EXCLUSIVE LOCK (NO READ/WRITE ARE POSSIBLE IN THE MEANTIME)
    return lock.executeInWriteLock(() -> {
      ((SchemaImpl) database.getSchema()).swapIndexes(mutable, newIndex);
      return newIndex;
    });
  }

  @Override
  public void close() {
    lock.executeInWriteLock(() -> {
      if (mutable != null)
        mutable.close();
      return null;
    });
  }

  @Override
  public String getName() {
    return mutable.getName();
  }

  @Override
  public IndexCursor iterator(final Object[] fromKeys) {
    return lock.executeInReadLock(() -> mutable.iterator(fromKeys));
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    return lock.executeInReadLock(() -> mutable.iterator(ascendingOrder));
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys) throws IOException {
    return lock.executeInReadLock(() -> mutable.iterator(ascendingOrder, fromKeys));
  }

  @Override
  public IndexCursor range(final Object[] beginKeys, final Object[] endKeys) throws IOException {
    return lock.executeInReadLock(() -> mutable.range(beginKeys, endKeys));
  }

  @Override
  public Set<RID> get(final Object[] keys) {
    return lock.executeInReadLock(() -> mutable.get(keys));
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    return lock.executeInReadLock(() -> mutable.get(keys, limit));
  }

  @Override
  public void put(final Object[] keys, final RID rid) {
    lock.executeInReadLock(() -> {
      mutable.put(keys, rid);
      return null;
    });
  }

  @Override
  public void put(final Object[] keys, final RID rid, final boolean checkForUnique) {
    lock.executeInReadLock(() -> {
      mutable.put(keys, rid, checkForUnique);
      return null;
    });
  }

  @Override
  public void remove(final Object[] keys) {
    lock.executeInReadLock(() -> {
      mutable.remove(keys);
      return null;
    });
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    lock.executeInReadLock(() -> {
      mutable.remove(keys, rid);
      return null;
    });
  }

  @Override
  public Map<String, Long> getStats() {
    return mutable.getStats();
  }

  @Override
  public int getFileId() {
    return mutable.getFileId();
  }

  @Override
  public boolean isUnique() {
    return mutable.isUnique();
  }

  public LSMTreeIndexMutable getMutableIndex() {
    return mutable;
  }

  @Override
  public PaginatedComponent getPaginatedComponent() {
    return mutable;
  }
}
