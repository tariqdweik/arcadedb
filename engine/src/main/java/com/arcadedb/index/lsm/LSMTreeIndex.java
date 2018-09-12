/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LSM-Tree index implementation. It relies on a mutable index and its underlying immutable, compacted index.
 */
public class LSMTreeIndex implements Index {
  private final String                                                  name;
  private       int                                                     associatedBucketId = -1;
  private       String                                                  typeName;
  private       String[]                                                propertyNames;
  private       LSMTreeIndexMutable                                     mutable;
  private       RWLockContext                                           lock               = new RWLockContext();
  protected     AtomicReference<LSMTreeIndexAbstract.COMPACTING_STATUS> compactingStatus   = new AtomicReference<>(LSMTreeIndexAbstract.COMPACTING_STATUS.NO);

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize) throws IOException {
      return new LSMTreeIndex(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize) throws IOException {
      final LSMTreeIndex mainIndex = new LSMTreeIndex(database, name, true, filePath, id, mode, pageSize);
      return mainIndex.mutable;
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize) throws IOException {
      final LSMTreeIndex mainIndex = new LSMTreeIndex(database, name, false, filePath, id, mode, pageSize);
      return new LSMTreeIndexMutable(mainIndex, database, name, false, filePath, id, mode, pageSize);
    }
  }

  /**
   * Called at creation time.
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    this.name = name;
    mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, mode, keyTypes, pageSize);
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this.name = name;
    mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, id, mode, pageSize);
  }

  public boolean scheduleCompaction() {
    return compactingStatus.compareAndSet(LSMTreeIndexAbstract.COMPACTING_STATUS.NO, LSMTreeIndexAbstract.COMPACTING_STATUS.SCHEDULED);
  }

  public void setMetadata(final String typeName, final String[] propertyNames, final int associatedBucketId) {
    this.typeName = typeName;
    this.propertyNames = propertyNames;
    this.associatedBucketId = associatedBucketId;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public String[] getPropertyNames() {
    return propertyNames;
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

  @Override
  public boolean isCompacting() {
    return compactingStatus.get() == LSMTreeIndexAbstract.COMPACTING_STATUS.IN_PROGRESS;
  }

  @Override
  public void close() {
    lock.executeInWriteLock(() -> {
      if (mutable != null)
        mutable.close();
      return null;
    });
  }

  public void drop() {
    lock.executeInWriteLock(() -> {
      ((SchemaImpl) mutable.getDatabase().getSchema()).removeIndex(getName());
      final LSMTreeIndexCompacted subIndex = mutable.getSubIndex();
      if (subIndex != null)
        subIndex.drop();

      mutable.drop();

      return null;
    });
  }

  @Override
  public String getName() {
    return name;
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
    return get(keys, -1);
  }

  @Override
  public Set<RID> get(final Object[] keys, final int limit) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      Set<RID> txChanges = null;

      final List<TransactionIndexContext.IndexKey> indexChanges = mutable.getDatabase().getTransaction().getIndexChanges().getIndexKeys(getName());
      if (indexChanges != null)
        for (int i = indexChanges.size() - 1; i > -1; --i) {
          final TransactionIndexContext.IndexKey indexChange = indexChanges.get(i);
          if (Arrays.equals(keys, indexChange.keyValues)) {
            if (!indexChange.addOperation)
              // REMOVED
              return Collections.EMPTY_SET;

            if (txChanges == null)
              txChanges = new HashSet<>();

            txChanges.add(indexChange.rid);

            if (limit > -1 && txChanges.size() > limit)
              // LIMIT REACHED
              return txChanges;
          }
        }

      final Set<RID> result = lock.executeInReadLock(() -> mutable.get(keys, limit));

      if (txChanges != null) {
        // MERGE SETS
        txChanges.addAll(result);
        return txChanges;
      }

      return result;
    }

    return lock.executeInReadLock(() -> mutable.get(keys, limit));
  }

  @Override
  public void put(final Object[] keys, final RID rid) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY ADDED AT COMMIT TIME (IN A LOCK)
      mutable.getDatabase().getTransaction().addIndexOperation(this, true, keys, rid);
    else
      lock.executeInReadLock(() -> {
        mutable.put(keys, rid);
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY REMOVED AT COMMIT TIME (IN A LOCK)
      mutable.getDatabase().getTransaction().addIndexOperation(this, false, keys, null);
    else
      lock.executeInReadLock(() -> {
        mutable.remove(keys);
        return null;
      });
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY REMOVED AT COMMIT TIME (IN A LOCK)
      mutable.getDatabase().getTransaction().addIndexOperation(this, false, keys, rid);
    else
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

  @Override
  public int getAssociatedBucketId() {
    return associatedBucketId;
  }

  @Override
  public String toString() {
    return name;
  }

  protected LSMTreeIndexMutable splitIndex(final int startingFromPage, final LSMTreeIndexCompacted subIndex) {
    final DatabaseInternal database = mutable.getDatabase();

    final int fileId = mutable.getFileId();

    database.getTransactionManager().tryLockFile(fileId, 0);

    try {
      // COPY MUTABLE PAGES TO THE NEW FILE
      return lock.executeInWriteLock(() -> {
        final int pageSize = mutable.getPageSize();

        int last_ = mutable.getName().lastIndexOf('_');
        final String newName = mutable.getName().substring(0, last_) + "_" + System.nanoTime();

        final LSMTreeIndexMutable newMutableIndex = new LSMTreeIndexMutable(this, database, newName, mutable.isUnique(),
            database.getDatabasePath() + "/" + newName, mutable.getKeyTypes(), pageSize, subIndex);
        ((SchemaImpl) database.getSchema()).registerFile(newMutableIndex);

        final MutablePage subIndexMainPage = subIndex.setCompactedTotalPages();
        database.getPageManager().updatePage(subIndexMainPage, false, false);

        // KEEP METADATA AND LEAVE IT EMPTY
        final MutablePage rootPage = newMutableIndex.createNewPage();
        database.getPageManager().updatePage(rootPage, true, false);
        newMutableIndex.setPageCount(1);

        for (int i = 0; i < mutable.getTotalPages() - startingFromPage; ++i) {
          final BasePage currentPage = database.getTransaction().getPage(new PageId(mutable.getFileId(), i + startingFromPage), pageSize);

          // COPY THE ENTIRE PAGE TO THE NEW INDEX
          final MutablePage newPage = newMutableIndex.createNewPage();

          final ByteBuffer pageContent = currentPage.getContent();
          pageContent.rewind();
          newPage.getContent().put(pageContent);

          database.getPageManager().updatePage(newPage, true, false);
          newMutableIndex.setPageCount(i + 2);
        }

        newMutableIndex.setCurrentMutablePages(newMutableIndex.getTotalPages() - 1);

        // SWAP OLD WITH NEW INDEX IN EXCLUSIVE LOCK (NO READ/WRITE ARE POSSIBLE IN THE MEANTIME)
        newMutableIndex.removeTempSuffix();

        mutable.drop();
        mutable = newMutableIndex;

        return newMutableIndex;
      });
    } finally {
      database.getTransactionManager().unlockFile(fileId);
    }
  }
}
