/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.*;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.*;
import com.arcadedb.graph.*;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.sql.executor.SQLEngine;
import com.arcadedb.sql.parser.ExecutionPlanCache;
import com.arcadedb.sql.parser.Statement;
import com.arcadedb.sql.parser.StatementCache;
import com.arcadedb.utility.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddedDatabase extends RWLockContext implements DatabaseInternal {
  protected final String                name;
  protected final PaginatedFile.MODE    mode;
  protected final ContextConfiguration  configuration;
  protected final String                databasePath;
  protected       FileManager           fileManager;
  protected       PageManager           pageManager;
  protected final BinarySerializer      serializer     = new BinarySerializer();
  protected final RecordFactory         recordFactory  = new RecordFactory();
  protected       SchemaImpl            schema;
  protected final GraphEngine           graphEngine    = new GraphEngine();
  protected       TransactionManager    transactionManager;
  protected final WALFileFactory        walFactory;
  protected       DatabaseAsyncExecutor asynch         = null;
  protected final DocumentIndexer       indexer;
  private         boolean               readYourWrites = true;

  protected          boolean autoTransaction = false;
  protected volatile boolean open            = false;

  protected static final Set<String>                               SUPPORTED_FILE_EXT      = new HashSet<String>(Arrays
      .asList(Dictionary.DICT_EXT, Bucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
          LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT));
  private                File                                      lockFile;
  private                FileLock                                  lockFileIO;
  private                Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks;
  private                StatementCache                            statementCache;
  private                ExecutionPlanCache                        executionPlanCache;
  private                DatabaseInternal                          wrappedDatabaseInstance = this;

  // STATISTICS
  private AtomicLong statsTxCommits     = new AtomicLong();
  private AtomicLong statsTxRollbacks   = new AtomicLong();
  private AtomicLong statsCreateRecord  = new AtomicLong();
  private AtomicLong statsReadRecord    = new AtomicLong();
  private AtomicLong statsUpdateRecord  = new AtomicLong();
  private AtomicLong statsDeleteRecord  = new AtomicLong();
  private AtomicLong statsQueries       = new AtomicLong();
  private AtomicLong statsCommands      = new AtomicLong();
  private AtomicLong statsScanType      = new AtomicLong();
  private AtomicLong statsScanBucket    = new AtomicLong();
  private AtomicLong statsIterateType   = new AtomicLong();
  private AtomicLong statsIterateBucket = new AtomicLong();
  private AtomicLong statsCountType     = new AtomicLong();
  private AtomicLong statsCountBucket   = new AtomicLong();

  protected EmbeddedDatabase(final String path, final PaginatedFile.MODE mode, final ContextConfiguration configuration,
      final Map<CALLBACK_EVENT, List<Callable<Void>>> callbacks) {
    try {
      this.mode = mode;
      this.configuration = configuration;
      this.callbacks = callbacks;
      this.walFactory = mode == PaginatedFile.MODE.READ_WRITE ? new WALFileFactoryEmbedded() : null;
      this.statementCache = new StatementCache(this, configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));
      this.executionPlanCache = new ExecutionPlanCache(this, configuration.getValueAsInteger(GlobalConfiguration.SQL_STATEMENT_CACHE));

      if (path.endsWith("/"))
        databasePath = path.substring(0, path.length() - 1);
      else
        databasePath = path;

      final int lastSeparatorPos = path.lastIndexOf("/");
      if (lastSeparatorPos > -1)
        name = path.substring(lastSeparatorPos + 1);
      else
        name = path;

      indexer = new DocumentIndexer(this);

    } catch (Exception e) {
      if (e instanceof DatabaseOperationException)
        throw (DatabaseOperationException) e;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  protected void open() {
    if (!new File(databasePath).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' not exists");

    openInternal();
  }

  protected void create() {
    if (new File(databasePath + "/" + SchemaImpl.SCHEMA_FILE_NAME).exists())
      throw new DatabaseOperationException("Database '" + databasePath + "' already exists");

    openInternal();

    schema.saveConfiguration();
  }

  private void openInternal() {
    try {
      DatabaseContext.INSTANCE.init(this);

      fileManager = new FileManager(databasePath, mode, SUPPORTED_FILE_EXT);
      transactionManager = new TransactionManager(wrappedDatabaseInstance);
      pageManager = new PageManager(fileManager, transactionManager, configuration);

      open = true;

      try {
        schema = new SchemaImpl(wrappedDatabaseInstance, databasePath, mode);

        if (fileManager.getFiles().isEmpty())
          schema.create(mode);
        else
          schema.load(mode);

        checkForRecovery();

        asynch = new DatabaseAsyncExecutor(wrappedDatabaseInstance);

        Profiler.INSTANCE.registerDatabase(this);

      } catch (RuntimeException e) {
        open = false;
        pageManager.close();
        throw e;
      } catch (Exception e) {
        open = false;
        pageManager.close();
        throw new DatabaseOperationException("Error on creating new database instance", e);
      }
    } catch (Exception e) {
      open = false;

      if (e instanceof DatabaseOperationException)
        throw (DatabaseOperationException) e;

      throw new DatabaseOperationException("Error on creating new database instance", e);
    }
  }

  private void checkForRecovery() throws IOException {
    lockFile = new File(databasePath + "/database.lck");

    if (lockFile.exists()) {
      lockDatabase();

      // RECOVERY
      LogManager.instance().warn(this, "Database '%s' was not closed properly last time", name);

      if (mode == PaginatedFile.MODE.READ_ONLY)
        throw new DatabaseMetadataException("Database needs recovery but has been open in read only mode");

      executeCallbacks(CALLBACK_EVENT.DB_NOT_CLOSED);

      transactionManager.checkIntegrity();
    } else {
      if (mode == PaginatedFile.MODE.READ_WRITE) {
        lockFile.createNewFile();
        lockDatabase();
      } else
        lockFile = null;
    }
  }

  @Override
  public void drop() {
    checkDatabaseIsOpen();
    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot drop database");

    close();

    executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        FileUtils.deleteRecursively(new File(databasePath));
        return null;
      }
    });
  }

  @Override
  public void close() {
    if (asynch != null) {
      // EXECUTE OUTSIDE LOCK
      asynch.waitCompletion();
      asynch.close();
    }

    executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (!open)
          return null;

        open = false;

        if (asynch != null)
          asynch.close();

        final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
        if (dbContext != null && dbContext.transaction != null) {
          if (dbContext.transaction != null && dbContext.transaction.isActive())
            // ROLLBACK ANY PENDING OPERATION
            dbContext.transaction.rollback();
        }

        try {
          schema.close();
          pageManager.close();
          fileManager.close();
          transactionManager.close();
          statementCache.clear();

          if (lockFile != null) {
            try {
              lockFileIO.release();
            } catch (IOException e) {
              // IGNORE IT
            }
            lockFile.delete();
          }

        } finally {
          DatabaseContext.INSTANCE.remove();
          Profiler.INSTANCE.unregisterDatabase(EmbeddedDatabase.this);
        }
        return null;
      }
    });
  }

  public DatabaseAsyncExecutor asynch() {
    return asynch;
  }

  @Override
  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("txCommits", statsTxCommits.get());
    map.put("txRollbacks", statsTxRollbacks.get());
    map.put("createRecord", statsCreateRecord.get());
    map.put("readRecord", statsReadRecord.get());
    map.put("updateRecord", statsUpdateRecord.get());
    map.put("deleteRecord", statsDeleteRecord.get());
    map.put("queries", statsQueries.get());
    map.put("commands", statsCommands.get());
    map.put("scanType", statsScanType.get());
    map.put("scanBucket", statsScanBucket.get());
    map.put("iterateType", statsIterateType.get());
    map.put("iterateBucket", statsIterateBucket.get());
    map.put("countType", statsCountType.get());
    map.put("countBucket", statsCountBucket.get());
    return map;
  }

  @Override
  public String getDatabasePath() {
    return databasePath;
  }

  public TransactionContext getTransaction() {
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContext(databasePath);
    if (dbContext != null) {
      final TransactionContext tx = dbContext.transaction;
      if (tx != null) {
        final DatabaseInternal txDb = tx.getDatabase();
        if (txDb == null)
          throw new TransactionException("Invalid transactional context (db is null)");
        if (txDb.getEmbedded() != this)
          throw new TransactionException("Invalid transactional context (different db)");
        return tx;
      }
    }
    return null;
  }

  @Override
  public void begin() {
    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        checkDatabaseIsOpen();

        // FORCE THE RESET OF TL
        DatabaseContext.INSTANCE.init(wrappedDatabaseInstance);

        getTransaction().begin();
        return null;
      }
    });
  }

  public void incrementStatsTxCommits() {
    statsTxCommits.incrementAndGet();
  }

  @Override
  public void commit() {
    statsTxCommits.incrementAndGet();

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        checkTransactionIsActive();
        getTransaction().commit();
        return null;
      }
    });
  }

  @Override
  public void rollback() {
    statsTxRollbacks.incrementAndGet();

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          checkTransactionIsActive();
          getTransaction().rollback();
        } catch (TransactionException e) {
          // ALREADY ROLLBACKED
        }
        return null;
      }
    });
  }

  @Override
  public long countBucket(final String bucketName) {
    statsCountBucket.incrementAndGet();

    return (Long) executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        checkTransactionIsActive();
        return schema.getBucketByName(bucketName).count();
      }
    });
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    statsCountType.incrementAndGet();

    return (Long) executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        checkTransactionIsActive();
        final DocumentType type = schema.getType(typeName);

        long total = 0;
        for (Bucket b : type.getBuckets(polymorphic))
          total += b.count();

        return total;
      }
    });
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    statsScanType.incrementAndGet();

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        checkTransactionIsActive();

        final DocumentType type = schema.getType(typeName);

        for (Bucket b : type.getBuckets(polymorphic)) {
          b.scan(new RawRecordCallback() {
            @Override
            public boolean onRecord(final RID rid, final Binary view) {
              final Document record = (Document) recordFactory.newImmutableRecord(wrappedDatabaseInstance, typeName, rid, view);
              return callback.onRecord(record);
            }
          });
        }
        return null;
      }
    });
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    statsScanBucket.incrementAndGet();

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        checkDatabaseIsOpen();

        final String type = schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getId());
        schema.getBucketByName(bucketName).scan(new RawRecordCallback() {
          @Override
          public boolean onRecord(final RID rid, final Binary view) {
            final Record record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type, rid, view);
            return callback.onRecord(record);
          }
        });
        return null;
      }
    });
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    statsIterateType.incrementAndGet();

    return (Iterator<Record>) executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        checkDatabaseIsOpen();
        try {
          final DocumentType type = schema.getType(typeName);

          final MultiIterator iter = new MultiIterator();

          for (Bucket b : type.getBuckets(polymorphic))
            iter.add(b.iterator());

          return iter;

        } catch (IOException e) {
          throw new DatabaseOperationException("Error on executing scan of type '" + schema.getType(typeName) + "'", e);
        }
      }
    });
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    statsIterateBucket.incrementAndGet();

    readLock();
    try {

      checkDatabaseIsOpen();
      try {
        final Bucket bucket = schema.getBucketByName(bucketName);
        return bucket.iterator();
      } catch (Exception e) {
        throw new DatabaseOperationException("Error on executing scan of bucket '" + bucketName + "'", e);
      }

    } finally {
      readUnlock();
    }
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    if (rid == null)
      throw new IllegalArgumentException("rid is null");

    statsReadRecord.incrementAndGet();

    return (Record) executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        checkDatabaseIsOpen();

        // CHECK IN TX CACHE FIRST
        final TransactionContext tx = getTransaction();
        Record record = tx.getRecordFromCache(rid);
        if (record != null)
          return record;

        final DocumentType type = schema.getTypeByBucketId(rid.getBucketId());

        if (loadContent) {
          final Binary buffer = schema.getBucketById(rid.getBucketId()).getRecord(rid);
          record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type != null ? type.getName() : null, rid, buffer);
          return record;
        }

        if (type != null)
          record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, type.getName(), rid, type.getType());
        else
          record = recordFactory.newImmutableRecord(wrappedDatabaseInstance, null, rid, Document.RECORD_TYPE);

        return record;
      }
    });
  }

  @Override
  public Cursor<RID> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    statsReadRecord.incrementAndGet();

    return (Cursor<RID>) executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        checkDatabaseIsOpen();
        final DocumentType t = schema.getType(type);

        final List<DocumentType.IndexMetadata> metadata = t.getIndexMetadataByProperties(properties);
        if (metadata == null || metadata.isEmpty())
          throw new IllegalArgumentException("No index has been created on type '" + type + "' properties " + Arrays.toString(properties));

        final Set<RID> result = new HashSet<>();
        for (DocumentType.IndexMetadata m : metadata) {
          final IndexCursor cursor = m.index.get(keys);
          while (cursor.hasNext())
            result.add(cursor.next());
        }

        return new CursorCollection<>(result);
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
  public GraphEngine getGraphEngine() {
    return graphEngine;
  }

  @Override
  public TransactionManager getTransactionManager() {
    return transactionManager;
  }

  @Override
  public boolean isReadYourWrites() {
    return readYourWrites;
  }

  @Override
  public void setReadYourWrites(final boolean readYourWrites) {
    this.readYourWrites = readYourWrites;
  }

  @Override
  public void createRecord(final MutableDocument record) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already persistent");

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {

        final boolean begunHere = checkTransactionIsActive();

        if (mode == PaginatedFile.MODE.READ_ONLY)
          throw new DatabaseIsReadOnlyException("Cannot create a new record");

        final DocumentType type = schema.getType(record.getType());

        final Bucket bucket = type.getBucketToSave(DatabaseContext.INSTANCE.getContext(databasePath).asyncMode);
        record.setIdentity(bucket.createRecord(record));
        indexer.createDocument(record, type, bucket);

        getTransaction().updateRecordInCache(record);

        if (begunHere)
          wrappedDatabaseInstance.commit();

        return null;
      }
    });
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        createRecordNoLock(record, bucketName);
        return null;
      }
    });
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName) {
    if (record.getIdentity() != null)
      throw new IllegalArgumentException("Cannot create record " + record.getIdentity() + " because it is already persistent");

    final boolean begunHere = checkTransactionIsActive();

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot create a new record");

    final Bucket bucket = schema.getBucketByName(bucketName);

    ((RecordInternal) record).setIdentity(bucket.createRecord(record));

    getTransaction().updateRecordInCache(record);

    if (begunHere)
      wrappedDatabaseInstance.commit();
  }

  @Override
  public void updateRecord(final Record record) {
    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        updateRecordNoLock(record);
        return null;
      }
    });
  }

  @Override
  public void updateRecordNoLock(final Record record) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot update the record because it is not persistent");

    final boolean begunHere = checkTransactionIsActive();

    if (mode == PaginatedFile.MODE.READ_ONLY)
      throw new DatabaseIsReadOnlyException("Cannot update a record");

    final Document originalRecord;
    if (record instanceof Document) {
      final Binary originalBuffer = ((RecordInternal) record).getBuffer();
      if (originalBuffer == null)
        throw new IllegalStateException("Cannot read original buffer for indexing");
      originalBuffer.rewind();
      originalRecord = (Document) recordFactory.newImmutableRecord(this, ((Document) record).getType(), record.getIdentity(), originalBuffer);
    } else
      originalRecord = null;

    schema.getBucketById(record.getIdentity().getBucketId()).updateRecord(record);

    if (record instanceof Document)
      indexer.updateDocument(originalRecord, (Document) record);

    getTransaction().updateRecordInCache(record);
    getTransaction().removeImmutableRecordsOfSamePage(record.getIdentity());

    if (begunHere)
      wrappedDatabaseInstance.commit();
  }

  @Override
  public void deleteRecord(final Record record) {
    if (record.getIdentity() == null)
      throw new IllegalArgumentException("Cannot delete a non persistent record");

    executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        final boolean begunHere = checkTransactionIsActive();

        if (mode == PaginatedFile.MODE.READ_ONLY)
          throw new DatabaseIsReadOnlyException("Cannot delete record " + record.getIdentity());

        final Bucket bucket = schema.getBucketById(record.getIdentity().getBucketId());

        if (record instanceof Document)
          indexer.deleteDocument((Document) record);

        if (record instanceof Edge) {
          graphEngine.deleteEdge((Edge) record);
        } else if (record instanceof Vertex) {
          graphEngine.deleteVertex((VertexInternal) record);
        } else
          bucket.deleteRecord(record.getIdentity());

        getTransaction().removeRecordFromCache(record);
        getTransaction().removeImmutableRecordsOfSamePage(record.getIdentity());

        if (begunHere)
          wrappedDatabaseInstance.commit();

        return null;
      }
    });
  }

  @Override
  public boolean isTransactionActive() {
    final Transaction tx = getTransaction();
    return tx != null && tx.isActive();
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    transaction(txBlock, configuration.getValueAsInteger(GlobalConfiguration.MVCC_RETRIES));
  }

  @Override
  public void transaction(final TransactionScope txBlock, int retries) {
    if (txBlock == null)
      throw new IllegalArgumentException("Transaction block is null");

    ConcurrentModificationException lastException = null;

    if (retries < 1)
      retries = 1;

    for (int retry = 0; retry < retries; ++retry) {
      try {
        wrappedDatabaseInstance.begin();
        txBlock.execute(wrappedDatabaseInstance);
        wrappedDatabaseInstance.commit();

        // OK
        return;

      } catch (ConcurrentModificationException e) {
        // RETRY
        lastException = e;
        continue;
      } catch (Exception e) {
        final Transaction tx = getTransaction();
        if (tx != null && tx.isActive())
          rollback();
        throw e;
      }
    }
    throw lastException;
  }

  @Override
  public RecordFactory getRecordFactory() {
    return recordFactory;
  }

  @Override
  public Schema getSchema() {
    checkDatabaseIsOpen();
    return schema;
  }

  @Override
  public BinarySerializer getSerializer() {
    return serializer;
  }

  @Override
  public PageManager getPageManager() {
    checkDatabaseIsOpen();
    return pageManager;
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final DocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(DocumentType.class))
      throw new IllegalArgumentException("Cannot create a document of type '" + typeName + "' because is not a document type");

    statsCreateRecord.incrementAndGet();

    return new MutableDocument(wrappedDatabaseInstance, typeName, null);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final DocumentType type = schema.getType(typeName);
    if (!type.getClass().equals(VertexType.class))
      throw new IllegalArgumentException("Cannot create a vertex of type '" + typeName + "' because is not a vertex type");

    statsCreateRecord.incrementAndGet();

    return new MutableVertex(wrappedDatabaseInstance, typeName, null);
  }

  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue, final String destinationVertexType,
      final String[] destinationVertexKey, final Object[] destinationVertexValue, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    if (sourceVertexKey == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKey.length != sourceVertexValue.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKey == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKey.length != destinationVertexValue.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<RID> v1Result = lookupByKey(sourceVertexType, sourceVertexKey, sourceVertexValue);

    Vertex sourceVertex;
    if (!v1Result.hasNext()) {
      if (createVertexIfNotExist) {
        sourceVertex = newVertex(sourceVertexType);
        for (int i = 0; i < sourceVertexKey.length; ++i)
          ((MutableVertex) sourceVertex).set(sourceVertexKey[i], sourceVertexValue[i]);
        ((MutableVertex) sourceVertex).save();
      } else
        throw new IllegalArgumentException("Cannot find source vertex with key " + Arrays.toString(sourceVertexKey) + "=" + Arrays.toString(sourceVertexValue));
    } else
      sourceVertex = (Vertex) v1Result.next().getRecord();

    final Iterator<RID> v2Result = lookupByKey(destinationVertexType, destinationVertexKey, destinationVertexValue);
    Vertex destinationVertex;
    if (!v2Result.hasNext()) {
      if (createVertexIfNotExist) {
        destinationVertex = newVertex(destinationVertexType);
        for (int i = 0; i < destinationVertexKey.length; ++i)
          ((MutableVertex) destinationVertex).set(destinationVertexKey[i], destinationVertexValue[i]);
        ((MutableVertex) destinationVertex).save();
      } else
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKey) + "=" + Arrays.toString(destinationVertexValue));
    } else
      destinationVertex = (Vertex) v2Result.next().getRecord();

    statsCreateRecord.incrementAndGet();

    return sourceVertex.newEdge(edgeType, destinationVertex, bidirectional, properties);
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    this.autoTransaction = autoTransaction;
  }

  @Override
  public FileManager getFileManager() {
    checkDatabaseIsOpen();
    return fileManager;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public PaginatedFile.MODE getMode() {
    return mode;
  }

  @Override
  public boolean checkTransactionIsActive() {
    checkDatabaseIsOpen();
    if (autoTransaction && !isTransactionActive()) {
      wrappedDatabaseInstance.begin();
      return true;
    } else if (!getTransaction().isActive())
      throw new DatabaseOperationException("Transaction not begun");

    return false;
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

      if (lockFile != null) {
        try {
          lockFileIO.release();
        } catch (IOException e) {
          // IGNORE IT
        }
      }

    } finally {
      open = false;
      Profiler.INSTANCE.unregisterDatabase(EmbeddedDatabase.this);
    }
  }

  @Override
  public DocumentIndexer getIndexer() {
    return indexer;
  }

  @Override
  public ResultSet command(String language, String query, final Object... args) {
    checkDatabaseIsOpen();

    statsCommands.incrementAndGet();

    final Statement statement = SQLEngine.parse(query, wrappedDatabaseInstance);
    final ResultSet original = statement.execute(wrappedDatabaseInstance, args);
    return original;
  }

  @Override
  public ResultSet command(String language, String query, final Map<String, Object> args) {
    checkDatabaseIsOpen();

    statsCommands.incrementAndGet();

    final Statement statement = SQLEngine.parse(query, wrappedDatabaseInstance);
    final ResultSet original = statement.execute(wrappedDatabaseInstance, args);
    return original;
  }

  @Override
  public ResultSet query(String language, String query, final Object... args) {
    checkDatabaseIsOpen();

    statsQueries.incrementAndGet();

    final Statement statement = SQLEngine.parse(query, wrappedDatabaseInstance);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");
    final ResultSet original = statement.execute(wrappedDatabaseInstance, args);
    return original;
  }

  @Override
  public ResultSet query(String language, String query, Map<String, Object> args) {
    checkDatabaseIsOpen();

    statsQueries.incrementAndGet();

    final Statement statement = SQLEngine.parse(query, wrappedDatabaseInstance);
    if (!statement.isIdempotent())
      throw new IllegalArgumentException("Query '" + query + "' is not idempotent");
    final ResultSet original = statement.execute(wrappedDatabaseInstance, args);
    return original;
  }

  /**
   * Returns true if two databases are the same.
   */
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final EmbeddedDatabase pDatabase = (EmbeddedDatabase) o;

    return databasePath != null ? databasePath.equals(pDatabase.databasePath) : pDatabase.databasePath == null;
  }

  public DatabaseContext.DatabaseContextTL getContext() {
    return DatabaseContext.INSTANCE.getContext(databasePath);
  }

  /**
   * Executes a callback in an shared lock.
   */
  @Override
  public <RET extends Object> RET executeInReadLock(final Callable<RET> callable) {
    readLock();
    try {

      return callable.call();

    } catch (ClosedChannelException e) {
      LogManager.instance().error(this, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new DatabaseOperationException("Error in execution in lock", e);

    } finally {
      readUnlock();
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  @Override
  public <RET extends Object> RET executeInWriteLock(final Callable<RET> callable) {
    writeLock();
    try {

      return callable.call();

    } catch (ClosedChannelException e) {
      LogManager.instance().error(this, "Database '%s' has some files that are closed", e, name);
      close();
      throw new DatabaseOperationException("Database '" + name + "' has some files that are closed", e);

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new DatabaseOperationException("Error in execution in lock", e);

    } finally {
      writeUnlock();
    }
  }

  @Override
  public StatementCache getStatementCache() {
    return statementCache;
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    return executionPlanCache;
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    return walFactory;
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
  public DatabaseInternal getEmbedded() {
    return this;
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public String toString() {
    return name;
  }

  public DatabaseInternal getWrappedDatabaseInstance() {
    return wrappedDatabaseInstance;
  }

  public void setWrappedDatabaseInstance(final DatabaseInternal wrappedDatabaseInstance) {
    this.wrappedDatabaseInstance = wrappedDatabaseInstance;
  }

  public static Object executeWithRetries(final com.arcadedb.utility.Callable<Object, Integer> callback, final int maxRetry) {
    return executeWithRetries(callback, maxRetry, 0, null);
  }

  public static Object executeWithRetries(final com.arcadedb.utility.Callable<Object, Integer> callback, final int maxRetry, final int waitBetweenRetry) {
    return executeWithRetries(callback, maxRetry, waitBetweenRetry, null);
  }

  public static Object executeWithRetries(final com.arcadedb.utility.Callable<Object, Integer> callback, final int maxRetry, final int waitBetweenRetry,
      final Record[] recordToReloadOnRetry) {
    NeedRetryException lastException = null;
    for (int retry = 0; retry < maxRetry; ++retry) {
      try {
        return callback.call(retry);
      } catch (NeedRetryException e) {
        // SAVE LAST EXCEPTION AND RETRY
        lastException = e;

        if (recordToReloadOnRetry != null) {
          // RELOAD THE RECORDS
          for (Record r : recordToReloadOnRetry)
            r.reload();
        }

        if (waitBetweenRetry > 0)
          try {
            Thread.sleep(waitBetweenRetry);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            break;
          }
      }
    }
    throw lastException;
  }

  protected void checkDatabaseIsOpen() {
    if (!open)
      throw new DatabaseIsClosedException(name);

    if (DatabaseContext.INSTANCE.get() == null)
      DatabaseContext.INSTANCE.init(wrappedDatabaseInstance);
  }

  private void lockDatabase() throws IOException {
    try {
      lockFileIO = new RandomAccessFile(lockFile, "rw").getChannel().tryLock();

      if (lockFileIO == null)
        throw new LockException("Database '" + name + "' is locked by another process");

    } catch (Exception e) {
      // IGNORE HERE
      throw new LockException("Database '" + name + "' is locked by another process", e);
    }
  }
}
