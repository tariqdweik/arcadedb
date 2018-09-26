/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.sql.executor.ResultSet;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

public interface Database extends AutoCloseable {
  interface TransactionScope {
    void execute(Database db);
  }

  ContextConfiguration getConfiguration();

  String getName();

  PaginatedFile.MODE getMode();

  @Override
  void close();

  boolean isOpen();

  void drop();

  DatabaseAsyncExecutor asynch();

  String getDatabasePath();

  TransactionContext getTransaction();

  boolean isTransactionActive();

  boolean checkTransactionIsActive();

  void transaction(TransactionScope txBlock);

  void transaction(TransactionScope txBlock, int retries);

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

  MutableDocument newDocument(String typeName);

  MutableVertex newVertex(String typeName);

  Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKey, Object[] sourceVertexValue, String destinationVertexType, String[] destinationVertexKey,
      Object[] destinationVertexValue, boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties);

  Schema getSchema();

  ResultSet command(String language, String query, Map<String, Object> args);

  ResultSet command(String language, String query, Object... args);

  ResultSet query(String language, String query, Object... args);

  ResultSet query(String language, String query, Map<String, Object> args);

  <RET extends Object> RET executeInReadLock(Callable<RET> callable);

  <RET extends Object> RET executeInWriteLock(Callable<RET> callable);

  boolean isReadYourWrites();

  void setReadYourWrites(boolean value);
}
