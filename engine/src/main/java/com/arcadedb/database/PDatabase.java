package com.arcadedb.database;

import com.arcadedb.database.async.PDatabaseAsyncExecutor;
import com.arcadedb.engine.PFileManager;
import com.arcadedb.engine.PPageManager;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.schema.PSchema;
import com.arcadedb.serializer.PBinarySerializer;
import com.arcadedb.sql.executor.OResultSet;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

public interface PDatabase {
  OResultSet command(String query, Map<String, Object> args);

  interface PTransaction {
    void execute(PDatabase database);
  }

  String getName();

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

  void scanType(String className, boolean polymorphic, PDocumentCallback callback);

  void scanBucket(String bucketName, PRecordCallback callback);

  PRecord lookupByRID(PRID rid, boolean loadContent);

  Iterator<PRecord> iterateType(String typeName, boolean polymorphic);

  Iterator<PRecord> iterateBucket(String bucketName);

  PCursor<PRID> lookupByKey(String type, String[] properties, Object[] keys);

  void deleteRecord(PRecord record);

  long countType(String typeName, boolean polymorphic);

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

  OResultSet query(String query, Map<String, Object> args);

  Object executeInReadLock(Callable<Object> callable);

  Object executeInWriteLock(Callable<Object> callable);

  boolean isReadYourWrites();

  void setReadYourWrites(boolean value);
}
