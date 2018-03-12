package com.arcadedb.database;

import com.arcadedb.PProfiler;
import com.arcadedb.engine.*;
import com.arcadedb.exception.PDatabaseIsClosedException;
import com.arcadedb.exception.PDatabaseIsReadOnlyException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.schema.PSchema;
import com.arcadedb.schema.PSchemaImpl;
import com.arcadedb.schema.PType;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLockContext;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class PDatabaseImpl extends PLockContext implements PDatabase {
  protected final String       name;
  protected final PFile.MODE   mode;
  protected final String       databasePath;
  protected final PFileManager fileManager;
  protected final PPageManager pageManager;
  protected final PBinarySerializer serializer    = new PBinarySerializer();
  protected final PRecordFactory    recordFactory = new PRecordFactory();
  protected final PSchemaImpl schema;

  protected          boolean autoTransaction = false;
  protected volatile boolean open            = false;

  protected static final Set<String> SUPPORTED_FILE_EXT = new HashSet<String>(
      Arrays.asList(PDictionary.DICT_EXT, PBucket.BUCKET_EXT, PIndex.INDEX_EXT));

  protected PDatabaseImpl(final String path, final PFile.MODE mode, final boolean multiThread) {
    super(multiThread);

    this.mode = mode;
    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;

    final int lastSeparatorPos = path.lastIndexOf("/");
    if (lastSeparatorPos > -1)
      name = path.substring(lastSeparatorPos + 1);
    else
      name = path;

    PTransactionTL.INSTANCE.set(new PTransactionContext(this));

    fileManager = new PFileManager(path, mode, SUPPORTED_FILE_EXT);
    pageManager = new PPageManager(fileManager);

    try {
      schema = new PSchemaImpl(this, databasePath, mode);

      PProfiler.INSTANCE.registerDatabase(this);

    } catch (RuntimeException e) {
      pageManager.close();
      throw e;
    } catch (Exception e) {
      pageManager.close();
      throw new PDatabaseOperationException("Error on creating new database instance", e);
    }

    open = true;
  }

  @Override
  public void drop() {
    lock();
    try {

      checkDatabaseIsOpen();
      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot drop database");

      close();
      PFileUtils.deleteRecursively(new File(databasePath));

    } finally {
      unlock();
    }
  }

  @Override
  public void close() {
    lock();
    try {

      if (!open)
        return;

      if (getTransaction().isActive())
        // ROLLBACK ANY PENDING OPERATION
        getTransaction().rollback();

      try {
        schema.close();
        pageManager.close();
        fileManager.close();
      } finally {
        open = false;
        PProfiler.INSTANCE.unregisterDatabase(this);
      }

    } finally {
      unlock();
    }
  }

  @Override
  public String getDatabasePath() {
    return databasePath;
  }

  public PTransactionContext getTransaction() {
    return PTransactionTL.INSTANCE.get();
  }

  @Override
  public void begin() {
    lock();
    try {

      checkDatabaseIsOpen();
      getTransaction().begin();

    } finally {
      unlock();
    }
  }

  @Override
  public void commit() {
    lock();
    try {

      checkTransactionIsActive();
      getTransaction().commit();

    } finally {
      unlock();
    }
  }

  @Override
  public void rollback() {
    lock();
    try {

      checkTransactionIsActive();
      getTransaction().rollback();

    } finally {
      unlock();
    }
  }

  @Override
  public int countBucket(String bucketName) {
    lock();
    try {

      checkDatabaseIsOpen();
      try {
        return schema.getBucketByName(bucketName).count();
      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on counting items in bucket '" + bucketName + "'", e);
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void scanBucket(final String bucketName, final PRecordCallback callback) {
    lock();
    try {

      checkDatabaseIsOpen();
      try {
        schema.getBucketByName(bucketName).scan(new PRawRecordCallback() {
          @Override
          public boolean onRecord(final PRID rid, final PBinary view) {
            unlock();
            try {

              final PRecord record = recordFactory.newImmutableRecord(PDatabaseImpl.this, rid, view);
              return callback.onRecord(record);

            } finally {
              lock();
            }
          }
        });
      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }

    } finally {
      unlock();
    }
  }

  @Override
  public PRecord lookupByRID(final PRID rid) {
    checkDatabaseIsOpen();
    lock();
    try {

      final PBinary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
      return recordFactory.newImmutableRecord(this, rid, buffer);

    } finally {
      unlock();
    }
  }

  @Override
  public List<? extends PRecord> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    checkDatabaseIsOpen();
    lock();
    try {

      final PType t = schema.getType(type);
      if (t == null)
        throw new IllegalArgumentException("Type '" + type + "' not exists");

      final List<PType.IndexMetadata> metadata = t.getIndexMetadataByProperties(properties);
      if (metadata == null || metadata.isEmpty())
        throw new IllegalArgumentException(
            "No index has been created on type '" + type + "' properties " + Arrays.toString(properties));

      for (PType.IndexMetadata m : metadata) {
        final List<PRID> rids = m.index.get(keys);
        if (!rids.isEmpty()) {
          final List<PRecord> result = new ArrayList<>();
          for (PRID rid : rids)
            result.add(lookupByRID(rid));
          return result;
        }
      }

    } finally {
      unlock();
    }
    return Collections.emptyList();
  }

  @Override
  public void saveRecord(final PModifiableDocument record) {
    lock();
    try {

      checkTransactionIsActive();
      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot save record");

      final PType type = schema.getType(record.getTypeName());
      if (type == null)
        throw new PDatabaseOperationException("Cannot save document because has no type");

      if (record.getIdentity() == null) {
        // NEW
        final PBucket bucket = type.getBucketToSave();
        record.setIdentity(bucket.addRecord(record));
        indexRecord(record, type, bucket);

      } else {
        // UPDATE
        // TODO
      }

    } finally {
      unlock();
    }
  }

  protected void indexRecord(final PModifiableDocument record, final PType type, final PBucket bucket) {
    // INDEX THE RECORD
    for (PType.IndexMetadata entry : type.getIndexMetadataByBucketId(bucket.getId())) {
      final PIndex index = entry.index;
      final String[] keyNames = entry.propertyNames;
      final Object[] keyValues = new Object[keyNames.length];
      for (int i = 0; i < keyNames.length; ++i) {
        keyValues[i] = record.get(keyNames[i]);
      }

      index.put(keyValues, record.getIdentity());
    }
  }

  @Override
  public void saveRecord(final PRecord record, final String bucketName) {
    lock();
    try {

      checkTransactionIsActive();
      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot save record");

      final PBucket bucket = schema.getBucketByName(bucketName);

      if (record.getIdentity() == null)
        // NEW
        ((PRecordInternal) record).setIdentity(bucket.addRecord(record));
      else {
        // UPDATE
        // TODO
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void deleteRecord(final PRID rid) {
    lock();
    try {

      checkTransactionIsActive();
      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot delete record " + rid);

      schema.getBucketById(rid.getBucketId()).deleteRecord(rid);

    } finally {
      unlock();
    }
  }

  @Override
  public boolean isTransactionActive() {
    return getTransaction().isActive();
  }

  @Override
  public void transaction(final PTransaction txBlock) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    lock();
    try {

      begin();
      txBlock.execute(this);
      commit();
    } catch (Exception e) {
      if (getTransaction().isActive())
        rollback();
      throw e;

    } finally {
      unlock();
    }
  }

  @Override
  public PRecordFactory getRecordFactory() {
    return recordFactory;
  }

  @Override
  public PSchema getSchema() {
    checkDatabaseIsOpen();
    return schema;
  }

  @Override
  public PBinarySerializer getSerializer() {
    return serializer;
  }

  @Override
  public PPageManager getPageManager() {
    return pageManager;
  }

  @Override
  public PModifiableDocument newDocument() {
    checkTransactionIsActive();
    return new PModifiableDocument(this, null);
  }

  @Override
  public PVertex newVertex() {
    //TODO support immutable/modifiable like for document
    checkTransactionIsActive();
    return new PVertex(this, null);
  }

  @Override
  public PEdge newEdge() {
    //TODO support immutable/modifiable like for document
    checkTransactionIsActive();
    return new PEdge(this, null);
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    this.autoTransaction = autoTransaction;
  }

  @Override
  public PFileManager getFileManager() {
    return fileManager;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PDatabaseImpl pDatabase = (PDatabaseImpl) o;

    return databasePath != null ? databasePath.equals(pDatabase.databasePath) : pDatabase.databasePath == null;
  }

  @Override
  public int hashCode() {
    return databasePath != null ? databasePath.hashCode() : 0;
  }

  private void checkDatabaseIsOpen() {
    if (!open)
      throw new PDatabaseIsClosedException(name);

    if (PTransactionTL.INSTANCE.get() == null)
      PTransactionTL.INSTANCE.set(new PTransactionContext(this));
  }

  @Override
  public void checkTransactionIsActive() {
    checkDatabaseIsOpen();
    if (autoTransaction && !isTransactionActive())
      begin();
    else if (!getTransaction().isActive())
      throw new PDatabaseOperationException("Transaction not begun");
  }
}
