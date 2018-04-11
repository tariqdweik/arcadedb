package com.arcadedb.database;

import com.arcadedb.PGlobalConfiguration;
import com.arcadedb.PProfiler;
import com.arcadedb.database.async.PDatabaseAsyncExecutor;
import com.arcadedb.engine.*;
import com.arcadedb.exception.*;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PGraphEngine;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexLSM;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PSchema;
import com.arcadedb.schema.PSchemaImpl;
import com.arcadedb.schema.PVertexType;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.sql.executor.OResultSet;
import com.arcadedb.sql.executor.OSQLEngine;
import com.arcadedb.sql.parser.Statement;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;
import com.arcadedb.utility.PRWLockContext;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class PDatabaseImpl extends PRWLockContext implements PDatabase, PDatabaseInternal {
  protected final String                 name;
  protected final PPaginatedFile.MODE    mode;
  protected final String                 databasePath;
  protected final PFileManager           fileManager;
  protected final PPageManager           pageManager;
  protected final PBinarySerializer      serializer    = new PBinarySerializer();
  protected final PRecordFactory         recordFactory = new PRecordFactory();
  protected final PSchemaImpl            schema;
  protected final PGraphEngine           graphEngine   = new PGraphEngine();
  protected final PTransactionManager    transactionManager;
  protected       PDatabaseAsyncExecutor asynch        = null;

  protected          boolean autoTransaction = false;
  protected volatile boolean open            = false;

  protected static final Set<String>                               SUPPORTED_FILE_EXT = new HashSet<String>(
      Arrays.asList(PDictionary.DICT_EXT, PBucket.BUCKET_EXT, PIndexLSM.INDEX_EXT));
  private                File                                      lockFile;
  private                Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks;

  protected PDatabaseImpl(final String path, final PPaginatedFile.MODE mode, final boolean multiThread,
      final Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks) {
    super(multiThread);

    try {
      this.mode = mode;
      this.callbacks = callbacks;
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
      transactionManager = new PTransactionManager(this);
      pageManager = new PPageManager(fileManager, transactionManager);

      open = true;

      try {
        schema = new PSchemaImpl(this, databasePath, mode);

        if (fileManager.getFiles().isEmpty())
          schema.create(mode);
        else
          schema.load(mode);

        checkForRecovery();

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
    } catch (Exception e) {
      open = false;

      if (e instanceof PDatabaseOperationException)
        throw (PDatabaseOperationException) e;

      throw new PDatabaseOperationException("Error on creating new database instance", e);
    }
  }

  private void checkForRecovery() throws IOException {
    lockFile = new File(databasePath + "/database.lck");

    if (lockFile.exists()) {
      // RECOVERY
      PLogManager.instance().warn(this, "Database '%s' was not closed properly last time", name);

      if (mode == PPaginatedFile.MODE.READ_ONLY)
        throw new PDatabaseMetadataException("Database needs recovery but has been open in read only mode");

      executeCallbacks(CALLBACK_EVENT.DB_NOT_CLOSED);

      transactionManager.checkIntegrity();
    } else
      lockFile.createNewFile();
  }

  @Override
  public void drop() {
    super.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkDatabaseIsOpen();
        if (mode == PPaginatedFile.MODE.READ_ONLY)
          throw new PDatabaseIsReadOnlyException("Cannot drop database");

        close();
        PFileUtils.deleteRecursively(new File(databasePath));
        return null;
      }
    });
  }

  @Override
  public void close() {
    super.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (!open)
          return null;

        if (asynch != null)
          asynch.close();

        if (getTransaction().isActive())
          // ROLLBACK ANY PENDING OPERATION
          getTransaction().rollback();

        try {
          schema.close();
          pageManager.close();
          fileManager.close();
          transactionManager.close();

          lockFile.delete();

        } finally {
          open = false;
          PProfiler.INSTANCE.unregisterDatabase(PDatabaseImpl.this);
        }
        return null;
      }
    });
  }

  public PDatabaseAsyncExecutor asynch() {
    if (asynch == null) {
      super.executeInWriteLock(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          asynch = new PDatabaseAsyncExecutor(PDatabaseImpl.this);
          return null;
        }
      });
    }
    return asynch;
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
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkDatabaseIsOpen();
        getTransaction().begin();
        return null;
      }
    });
  }

  @Override
  public void commit() {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkTransactionIsActive();
        getTransaction().commit();
        return null;
      }
    });
  }

  @Override
  public void rollback() {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkTransactionIsActive();
        getTransaction().rollback();
        return null;
      }
    });
  }

  @Override
  public long countBucket(final String bucketName) {
    return (Long) super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkDatabaseIsOpen();
        return schema.getBucketByName(bucketName).count();
      }
    });
  }

  @Override
  public long countType(final String typeName) {
    return (Long) super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkDatabaseIsOpen();
        final PDocumentType type = schema.getType(typeName);

        long total = 0;
        for (PBucket b : type.getBuckets()) {
          total += b.count();
        }
        return total;
      }
    });
  }

  @Override
  public void scanType(final String typeName, final PDocumentCallback callback) {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        checkDatabaseIsOpen();
        try {
          final PDocumentType type = schema.getType(typeName);

          for (PBucket b : type.getBuckets()) {
            b.scan(new PRawRecordCallback() {
              @Override
              public boolean onRecord(final PRID rid, final PBinary view) {
                final PDocument record = (PDocument) recordFactory.newImmutableRecord(PDatabaseImpl.this, typeName, rid, view);
                return callback.onRecord(record);
              }
            });
          }
        } catch (IOException e) {
          throw new PDatabaseOperationException("Error on executing scan of type '" + schema.getType(typeName) + "'", e);
        }
        return null;
      }
    });
  }

  @Override
  public void scanBucket(final String bucketName, final PRecordCallback callback) {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        checkDatabaseIsOpen();
        try {
          final String type = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getId());
          schema.getBucketByName(bucketName).scan(new PRawRecordCallback() {
            @Override
            public boolean onRecord(final PRID rid, final PBinary view) {
              final PRecord record = recordFactory.newImmutableRecord(PDatabaseImpl.this, type, rid, view);
              return callback.onRecord(record);
            }
          });
        } catch (IOException e) {
          throw new PDatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
        }
        return null;
      }
    });
  }

  @Override
  public Iterator<PRecord> bucketIterator(final String bucketName) {
    readLock();
    try {

      checkDatabaseIsOpen();
      try {
        PBucket bucket = schema.getBucketByName(bucketName);
        return bucket.iterator();
      } catch (Exception e) {
        throw new PDatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }

    } finally {
      readUnlock();
    }
  }

  @Override
  public PRecord lookupByRID(final PRID rid, final boolean loadContent) {
    return (PRecord) super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        checkDatabaseIsOpen();
        final PDocumentType type = schema.getTypeByBucketId(rid.getBucketId());

        if (loadContent) {
          final PBinary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
          return recordFactory.newImmutableRecord(PDatabaseImpl.this, type != null ? type.getName() : null, rid, buffer);
        }

        if (type != null)
          return recordFactory.newImmutableRecord(PDatabaseImpl.this, type.getName(), rid, type.getType());

        return recordFactory.newImmutableRecord(PDatabaseImpl.this, null, rid, PDocument.RECORD_TYPE);
      }
    });
  }

  @Override
  public PCursor<PRID> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    return (PCursor<PRID>) super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        checkDatabaseIsOpen();
        final PDocumentType t = schema.getType(type);

        final List<PDocumentType.IndexMetadata> metadata = t.getIndexMetadataByProperties(properties);
        if (metadata == null || metadata.isEmpty())
          throw new IllegalArgumentException(
              "No index has been created on type '" + type + "' properties " + Arrays.toString(properties));

        final List<PRID> result = new ArrayList<>();
        for (PDocumentType.IndexMetadata m : metadata)
          result.addAll(m.index.get(keys));

        return new PCursorCollection<PRID>(result);
      }
    });
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks == null) {
      callbacks = new ArrayList<Callable<Void>>();
      this.callbacks.put(event, callbacks);
    }
    callbacks.add(callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks != null) {
      callbacks.remove(callback);
      if (callbacks.isEmpty())
        this.callbacks.remove(event);
    }
  }

  @Override
  public PGraphEngine getGraphEngine() {
    return graphEngine;
  }

  @Override
  public PTransactionManager getTransactionManager() {
    return transactionManager;
  }

  @Override
  public void createRecord(final PModifiableDocument record) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already persistent");

    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        checkTransactionIsActive();
        if (mode == PPaginatedFile.MODE.READ_ONLY)
          throw new PDatabaseIsReadOnlyException("Cannot create a new record");

        final PDocumentType type = schema.getType(record.getType());

        // NEW
        final PBucket bucket = type.getBucketToSave();
        record.setIdentity(bucket.createRecord(record));
        indexDocument(record, type, bucket);
        return null;
      }
    });
  }

  @Override
  public void createRecord(final PRecord record, final String bucketName) {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        createRecordNoLock(record, bucketName);
        return null;
      }
    });
  }

  @Override
  public void createRecordNoLock(final PRecord record, final String bucketName) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already persistent");

    checkTransactionIsActive();
    if (mode == PPaginatedFile.MODE.READ_ONLY)
      throw new PDatabaseIsReadOnlyException("Cannot create a new record");

    final PBucket bucket = schema.getBucketByName(bucketName);

    ((PRecordInternal) record).setIdentity(bucket.createRecord(record));
  }

  @Override
  public void updateRecord(final PRecord record) {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        updateRecordNoLock(record);
        return null;
      }
    });
  }

  @Override
  public void updateRecordNoLock(final PRecord record) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot update the record because it is not persistent");

    checkTransactionIsActive();
    if (mode == PPaginatedFile.MODE.READ_ONLY)
      throw new PDatabaseIsReadOnlyException("Cannot update a record");

    schema.getBucketById(record.getIdentity().getBucketId()).update(record);
  }

  @Override
  public void deleteRecord(final PRID rid) {
    super.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        checkTransactionIsActive();
        if (mode == PPaginatedFile.MODE.READ_ONLY)
          throw new PDatabaseIsReadOnlyException("Cannot delete record " + rid);

        schema.getBucketById(rid.getBucketId()).deleteRecord(rid);
        return null;
      }
    });
  }

  @Override
  public boolean isTransactionActive() {
    return getTransaction().isActive();
  }

  @Override
  public void transaction(final PTransaction txBlock) {
    transaction(txBlock, PGlobalConfiguration.MVCC_RETRIES.getValueAsInteger());
  }

  @Override
  public void transaction(final PTransaction txBlock, final int retries) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    PConcurrentModificationException lastException = null;

    for (int retry = 0; retry < retries; ++retry) {
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
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final PDocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(PDocumentType.class))
      throw new IllegalArgumentException("Cannot create a document of type '" + typeName + "' because is not a document type");

    return new PModifiableDocument(this, typeName, null);
  }

  @Override
  public PModifiableVertex newVertex(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final PDocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(PVertexType.class))
      throw new IllegalArgumentException("Cannot create a vertex of type '" + typeName + "' because is not a vertex type");

    return new PModifiableVertex(this, typeName, null);
  }

  public PEdge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue,
      final String destinationVertexType, final String[] destinationVertexKey, final Object[] destinationVertexValue,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {
    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<PRID> v1Result = lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);

    PVertex sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKey.length; ++i)
          ((PModifiableVertex) sourceVertex).set(sourceVertexKey[i], sourceVertexValue[i]);
      } else
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));
    } else
      sourceVertex = (PVertex) v1Result.next().getRecord();

    final Iterator<PRID> v2Result = lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);
    PVertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKey.length; ++i)
          ((PModifiableVertex) destinationVertex).set(destinationVertexKey[i], destinationVertexValue[i]);
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays
                .toString(destinationVertexValue));
    } else
      destinationVertex = (PVertex) v2Result.next().getRecord();

    return sourceVertex.newEdge(edgeType, destinationVertex, bidirectional, properties);
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
  public void checkTransactionIsActive() {
    checkDatabaseIsOpen();
    if (autoTransaction && !isTransactionActive())
      begin();
    else if (!getTransaction().isActive())
      throw new PDatabaseOperationException("Transaction not begun");
  }

  @Override
  public void indexDocument(final PModifiableDocument record, final PDocumentType type, final PBucket bucket) {
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

  /**
   * Test only API.
   */
  @Override
  public void kill() {
    if (asynch != null)
      asynch.kill();

    if (getTransaction().isActive())
      // ROLLBACK ANY PENDING OPERATION
      getTransaction().kill();

    try {
      schema.close();
      pageManager.kill();
      fileManager.close();
      transactionManager.kill();

    } finally {
      open = false;
      PProfiler.INSTANCE.unregisterDatabase(PDatabaseImpl.this);
    }
  }

  @Override
  public OResultSet query(String query, Map<String, Object> args) {
    Statement statement = OSQLEngine.parse(query, this);
    if (!statement.isIdempotent()) {
      throw new PCommandExecutionException("Cannot execute query on non idempotent statement: " + query);
    }
    OResultSet original = statement.execute(this, args);
    return original;
//    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
//    this.queryStarted(result.getQueryId(), result);
//    result.addLifecycleListener(this);
//    return result;
  }

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

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    final List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks != null && !callbacks.isEmpty()) {
      for (Callable<Void> cb : callbacks) {
        try {
          cb.call();
        } catch (RuntimeException | IOException e) {
          throw e;
        } catch (Exception e) {
          throw new IOException("Error on executing test callback EVENT=" + event, e);
        }
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }

  protected void checkDatabaseIsOpen() {
    if (!open)
      throw new PDatabaseIsClosedException(name);

    if (PTransactionTL.INSTANCE.get() == null)
      PTransactionTL.INSTANCE.set(new PTransactionContext(this));
  }
}
