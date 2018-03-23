package com.arcadedb.database;

import com.arcadedb.PProfiler;
import com.arcadedb.engine.*;
import com.arcadedb.exception.PConcurrentModificationException;
import com.arcadedb.exception.PDatabaseIsClosedException;
import com.arcadedb.exception.PDatabaseIsReadOnlyException;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexLSM;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PSchema;
import com.arcadedb.schema.PSchemaImpl;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLockContext;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class PDatabaseImpl extends PLockContext implements PDatabase, PDatabaseInternal {
  private static final int DEFAULT_RETRIES = 10;

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
      Arrays.asList(PDictionary.DICT_EXT, PBucket.BUCKET_EXT, PIndexLSM.INDEX_EXT));

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

    open = true;

    try {
      schema = new PSchemaImpl(this, databasePath, mode);

      if (fileManager.getFiles().isEmpty())
        schema.create(mode);
      else
        schema.load(mode);

      PProfiler.INSTANCE.registerDatabase(this);

    } catch (RuntimeException e) {
      open = false;
      pageManager.close();
      throw e;
    } catch (Exception e) {
      open = false;
      pageManager.close();
      throw new PDatabaseOperationException("Error on creating new database instance", e);
    }
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
  public long countBucket(final String bucketName) {
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
  public long countType(final String typeName) {
    lock();
    try {

      checkDatabaseIsOpen();
      try {
        final PDocumentType type = schema.getType(typeName);

        long total = 0;
        for (PBucket b : type.getBuckets()) {
          total += b.count();
        }

        return total;

      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on counting items in type '" + typeName + "'", e);
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void scanType(final String typeName, final PRecordCallback callback) {
    lock();
    try {

      checkDatabaseIsOpen();
      try {
        final PDocumentType type = schema.getType(typeName);

        for (PBucket b : type.getBuckets()) {
          b.scan(new PRawRecordCallback() {
            @Override
            public boolean onRecord(final PRID rid, final PBinary view) {
              unlock();
              try {

                final PRecord record = recordFactory.newImmutableRecord(PDatabaseImpl.this, typeName, rid, view);
                return callback.onRecord(record);

              } finally {
                lock();
              }
            }
          });
        }
      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on executing scan of type '" + schema.getType(typeName) + "'", e);
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
        final String type = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getId());
        schema.getBucketByName(bucketName).scan(new PRawRecordCallback() {
          @Override
          public boolean onRecord(final PRID rid, final PBinary view) {
            unlock();
            try {

              final PRecord record = recordFactory.newImmutableRecord(PDatabaseImpl.this, type, rid, view);
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
  public Iterator<PRecord> bucketIterator(final String bucketName) {
    lock();
    try {

      checkDatabaseIsOpen();
      try {
        PBucket bucket = schema.getBucketByName(bucketName);
        return bucket.iterator();
      } catch (Exception e) {
        throw new PDatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }

    } finally {
      unlock();
    }
  }

  @Override
  public PRecord lookupByRID(final PRID rid, final boolean loadContent) {
    checkDatabaseIsOpen();
    lock();
    try {

      final PDocumentType type = schema.getTypeByBucketId(rid.getBucketId());

      if (loadContent) {
        final PBinary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
        return recordFactory.newImmutableRecord(this, type != null ? type.getName() : null, rid, buffer);
      }

      if (type != null)
        return recordFactory.newImmutableRecord(this, type.getName(), rid, type.getType());

      return recordFactory.newImmutableRecord(this, null, rid, PBaseRecord.RECORD_TYPE);

    } finally {
      unlock();
    }
  }

  @Override
  public PCursor<PRID> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    checkDatabaseIsOpen();
    lock();
    try {

      final PDocumentType t = schema.getType(type);

      final List<PDocumentType.IndexMetadata> metadata = t.getIndexMetadataByProperties(properties);
      if (metadata == null || metadata.isEmpty())
        throw new IllegalArgumentException(
            "No index has been created on type '" + type + "' properties " + Arrays.toString(properties));

      final List<PRID> result = new ArrayList<>();
      for (PDocumentType.IndexMetadata m : metadata)
        result.addAll(m.index.get(keys));

      return new PCursorCollection<PRID>(result);

    } finally {
      unlock();
    }
  }

  @Override
  public void saveRecord(final PModifiableDocument record) {
    lock();
    try {

      checkTransactionIsActive();
      if (mode == PFile.MODE.READ_ONLY)
        throw new PDatabaseIsReadOnlyException("Cannot save record");

      final PDocumentType type = schema.getType(record.getType());

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
    transaction(txBlock, DEFAULT_RETRIES);
  }

  @Override
  public void transaction(final PTransaction txBlock, final int retries) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    PConcurrentModificationException lastException = null;

    for (int retry = 0; retry < retries; ++retry) {
      lock();
      try {

        begin();
        txBlock.execute(this);
        commit();

        // OK
        return;

      } catch (PConcurrentModificationException e) {
        // RETRY
        lastException = e;
        continue;
      } catch (Exception e) {
        if (getTransaction().isActive())
          rollback();
        throw e;

      } finally {
        unlock();
      }
    }

    throw lastException;
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
  public PModifiableDocument newDocument(final String typeName) {
    checkTransactionIsActive();
    return new PModifiableDocument(this, typeName, null);
  }

  @Override
  public PModifiableVertex newVertex(final String typeName) {
    checkTransactionIsActive();
    return new PModifiableVertex(this, typeName, null);
  }

  @Override
  public PEdge newEdge(final String typeName) {
    checkTransactionIsActive();
    return new PModifiableEdge(this, typeName, null);
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

  protected void checkDatabaseIsOpen() {
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

  protected void indexRecord(final PModifiableDocument record, final PDocumentType type, final PBucket bucket) {
    // INDEX THE RECORD
    final List<PDocumentType.IndexMetadata> metadata = type.getIndexMetadataByBucketId(bucket.getId());
    if (metadata != null) {
      for (PDocumentType.IndexMetadata entry : metadata) {
        final PIndex index = entry.index;
        final String[] keyNames = entry.propertyNames;
        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; ++i) {
          keyValues[i] = record.get(keyNames[i]);
        }

        index.put(keyValues, record.getIdentity());
      }
    }
  }
}
