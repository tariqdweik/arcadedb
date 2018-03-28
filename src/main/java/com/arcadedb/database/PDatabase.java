package com.arcadedb.database;

import com.arcadedb.database.async.PDatabaseAsyncExecutor;
import com.arcadedb.engine.PFileManager;
import com.arcadedb.engine.PPageManager;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.schema.PSchema;
import com.arcadedb.serializer.PBinarySerializer;

import java.util.concurrent.Callable;

public interface PDatabase {
  interface PTransaction {
    void execute(PDatabase database);
  }

  void drop();

  void close();

  PDatabaseAsyncExecutor asynch();

  String getDatabasePath();

  PTransactionContext getTransaction();

  boolean isTransactionActive();

  void checkTransactionIsActive();

  void transaction(PTransaction txBlock);

  void setAutoTransaction(boolean autoTransaction);

  void begin();

  void commit();

  void rollback();

  void scanType(String className, PDocumentCallback callback);

  void scanBucket(String bucketName, PRecordCallback callback);

  PRecord lookupByRID(PRID rid, boolean loadContent);

  PCursor<PRID> lookupByKey(String type, String[] properties, Object[] keys);

  void deleteRecord(PRID rid);

  long countType(String typeName);

  long countBucket(String bucketName);

  PModifiableDocument newDocument(String typeName);

  PModifiableVertex newVertex(String typeName);

  PEdge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKey, Object[] sourceVertexValue, String destinationVertexType,
      String[] destinationVertexKey, Object[] destinationVertexValue, boolean createVertexIfNotExist, String edgeType,
      boolean bidirectional, Object... properties);

  PSchema getSchema();

  PFileManager getFileManager();

  void transaction(PTransaction txBlock, int retries);

  PRecordFactory getRecordFactory();

  PBinarySerializer getSerializer();

  PPageManager getPageManager();

  Object executeInReadLock(Callable<Object> callable);

  Object executeInWriteLock(Callable<Object> callable);
}
