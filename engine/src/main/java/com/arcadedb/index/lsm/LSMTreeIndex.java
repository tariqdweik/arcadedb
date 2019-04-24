/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.*;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.*;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.utility.RWLockContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LSM-Tree index implementation. It relies on a mutable index and its underlying immutable, compacted index.
 */
public class LSMTreeIndex implements RangeIndex {
  private static final IndexCursor                                             EMPTY_CURSOR       = new EmptyIndexCursor();
  private final        String                                                  name;
  private              int                                                     associatedBucketId = -1;
  private              String                                                  typeName;
  protected            String[]                                                propertyNames;
  protected            LSMTreeIndexMutable                                     mutable;
  private              RWLockContext                                           lock               = new RWLockContext();
  protected            AtomicReference<LSMTreeIndexAbstract.COMPACTING_STATUS> compactingStatus   = new AtomicReference<>(
      LSMTreeIndexAbstract.COMPACTING_STATUS.NO);

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public Index create(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
        final byte[] keyTypes, final int pageSize, BuildIndexCallback callback) throws IOException {
      return new LSMTreeIndex(database, name, unique, filePath, mode, keyTypes, pageSize);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize) throws IOException {
      if (filePath.endsWith(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT))
        return new LSMTreeIndexCompacted(null, database, name, true, filePath, id, mode, pageSize);

      return new LSMTreeIndex(database, name, true, filePath, id, mode, pageSize).mutable;
    }
  }

  public static class PaginatedComponentFactoryHandlerNotUnique implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final PaginatedFile.MODE mode, final int pageSize) throws IOException {
      if (filePath.endsWith(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT))
        return new LSMTreeIndexCompacted(null, database, name, false, filePath, id, mode, pageSize);

      return new LSMTreeIndex(database, name, false, filePath, id, mode, pageSize).mutable;
    }
  }

  /**
   * Called at creation time.
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    this.name = name;
    this.mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, mode, keyTypes, pageSize);
  }

  /**
   * Called at load time (1st page only).
   */
  public LSMTreeIndex(final DatabaseInternal database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this.name = name;
    this.mutable = new LSMTreeIndexMutable(this, database, name, unique, filePath, id, mode, pageSize);
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
  public boolean equals(final Object obj) {
    if (obj == this)
      return true;

    if (!(obj instanceof LSMTreeIndex))
      return false;

    final LSMTreeIndex m2 = (LSMTreeIndex) obj;

    if (!name.equals(m2.name))
      return false;

    if (!typeName.equals(m2.typeName))
      return false;

    if (associatedBucketId != m2.associatedBucketId)
      return false;

    if (!Arrays.equals(propertyNames, m2.propertyNames))
      return false;

    return true;
  }

  @Override
  public SchemaImpl.INDEX_TYPE getType() {
    return SchemaImpl.INDEX_TYPE.LSM_TREE;
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
  public IndexCursor iterator(final boolean ascendingOrder) {
    return lock.executeInReadLock(() -> new LSMTreeIndexCursor(mutable, ascendingOrder));
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    return lock.executeInReadLock(() -> mutable.iterator(ascendingOrder, fromKeys, inclusive));
  }

  @Override
  public IndexCursor range(final Object[] beginKeys, final boolean beginKeysInclusive, final Object[] endKeys, final boolean endKeysInclusive) {
    return lock.executeInReadLock(() -> mutable.range(beginKeys, beginKeysInclusive, endKeys, endKeysInclusive));
  }

  @Override
  public boolean supportsOrderedIterations() {
    return true;
  }

  @Override
  public boolean isAutomatic() {
    return propertyNames != null;
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      Set<IndexCursorEntry> txChanges = null;

      final Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>> indexChanges = mutable.getDatabase().getTransaction()
          .getIndexChanges().getIndexKeys(getName());
      if (indexChanges != null) {
        final Set<TransactionIndexContext.IndexKey> values = indexChanges.get(new TransactionIndexContext.ComparableKey(keys));
        if (values != null) {
          for (final TransactionIndexContext.IndexKey value : values) {
            if (value != null) {
              if (!value.addOperation)
                // REMOVED
                return EMPTY_CURSOR;

              if (txChanges == null)
                txChanges = new HashSet<>();

              txChanges.add(new IndexCursorEntry(keys, value.rid, 1));

              if (limit > -1 && txChanges.size() > limit)
                // LIMIT REACHED
                return new TempIndexCursor(txChanges);
            }
          }
        }
      }

      final IndexCursor result = lock.executeInReadLock(() -> mutable.get(keys, limit));

      if (txChanges != null) {
        // MERGE SETS
        while (result.hasNext())
          txChanges.add(new IndexCursorEntry(keys, result.next(), 1));
        return new TempIndexCursor(txChanges);
      }

      return result;
    }

    return lock.executeInReadLock(() -> mutable.get(keys, limit));
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      // KEY ADDED AT COMMIT TIME (IN A LOCK)
      final TransactionContext tx = mutable.getDatabase().getTransaction();
      for (RID rid : rids)
        tx.addIndexOperation(this, true, keys, rid);
    } else
      lock.executeInReadLock(() -> {
        mutable.put(keys, rids);
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
  public void remove(final Object[] keys, final Identifiable rid) {
    if (mutable.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN)
      // KEY REMOVED AT COMMIT TIME (IN A LOCK)
      mutable.getDatabase().getTransaction().addIndexOperation(this, false, keys, rid.getIdentity());
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

        ((SchemaImpl) database.getSchema()).saveConfiguration();

        return newMutableIndex;
      });
    } finally {
      database.getTransactionManager().unlockFile(fileId);
    }
  }

  public long build(final BuildIndexCallback callback) {
    final AtomicLong total = new AtomicLong();

    if (propertyNames == null || propertyNames.length == 0)
      throw new IndexException("Cannot rebuild index '" + name + "' because metadata information are missing");

    final DatabaseInternal db = mutable.getDatabase();

    final DocumentType type = db.getSchema().getType(typeName);

    db.scanBucket(db.getSchema().getBucketById(associatedBucketId).getName(), new RecordCallback() {
      @Override
      public boolean onRecord(final Record record) {
        final Bucket bucket = db.getSchema().getBucketById(record.getIdentity().getBucketId());
        db.getIndexer().createDocument((Document) record, type, bucket);
        total.incrementAndGet();

        if (callback != null)
          callback.onDocumentIndexed((Document) record, total.get());

        return true;
      }
    });

    return total.get();
  }
}
