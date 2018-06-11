/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.sql.executor.ResultSet;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

public interface Database {
  interface Transaction {
    void execute(Database database);
  }

  String getName();

  void drop();

  void close();

  DatabaseAsyncExecutor asynch();

  String getDatabasePath();

  TransactionContext getTransaction();

  boolean isTransactionActive();

  boolean checkTransactionIsActive();

  void transaction(Transaction txBlock);

  void setAutoTransaction(boolean autoTransaction);

  void begin();

  void commit();

  void rollback();

  void scanType(String className, boolean polymorphic, DocumentCallback callback);

  void scanBucket(String bucketName, RecordCallback callback);

  Record lookupByRID(RID rid, boolean loadContent);

  Iterator<Record> iterateType(String typeName, boolean polymorphic);

  Iterator<Record> iterateBucket(String bucketName);

  Cursor<RID> lookupByKey(String type, String[] properties, Object[] keys);

  void deleteRecord(Record record);

  long countType(String typeName, boolean polymorphic);

  long countBucket(String bucketName);

  ModifiableDocument newDocument(String typeName);

  ModifiableVertex newVertex(String typeName);

  Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKey, Object[] sourceVertexValue, String destinationVertexType,
      String[] destinationVertexKey, Object[] destinationVertexValue, boolean createVertexIfNotExist, String edgeType,
      boolean bidirectional, Object... properties);

  Schema getSchema();

  FileManager getFileManager();

  void transaction(Transaction txBlock, int retries);

  RecordFactory getRecordFactory();

  BinarySerializer getSerializer();

  PageManager getPageManager();

  ResultSet query(String query, Map<String, Object> args);

  ResultSet sql(String query, Map<String, Object> args);

  Object executeInReadLock(Callable<Object> callable);

  Object executeInWriteLock(Callable<Object> callable);

  boolean isReadYourWrites();

  void setReadYourWrites(boolean value);
}
