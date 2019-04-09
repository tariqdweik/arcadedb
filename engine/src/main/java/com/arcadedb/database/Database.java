/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
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

  DatabaseAsyncExecutor async();

  String getDatabasePath();

  TransactionContext getTransaction();

  boolean isTransactionActive();

  boolean checkTransactionIsActive();

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is joined and no commit is executed at the end
   * of the lambda execution. Ibstead, if there is no transaction running, then a new transacton is created and committed at the end of the lambda execution.
   *
   * @param txBlock
   */
  void transaction(TransactionScope txBlock);

  /**
   * Executes a lambda in the transaction scope. If there is an active transaction, then the current transaction is joined and no commit is executed at the end
   * of the lambda execution. Ibstead, if there is no transaction running, then a new transacton is created and committed at the end of the lambda execution.
   * The difference with the methos {@link #transaction(TransactionScope)} is that in case the NeedRetryException exception is thrown, the transaction is
   * re-executed for a number of retries.
   *
   * @param txBlock number of retries in case the NeedRetryException exception is thrown
   */
  void transaction(TransactionScope txBlock, int retries);

  void setAutoTransaction(boolean autoTransaction);

  void begin();

  void commit();

  void rollback();

  void scanType(String className, boolean polymorphic, DocumentCallback callback);

  void scanBucket(String bucketName, RecordCallback callback);

  Record lookupByRID(RID rid, boolean loadContent);

  IndexCursor lookupByKey(String type, String[] properties, Object[] keys);

  Iterator<Record> iterateType(String typeName, boolean polymorphic);

  Iterator<Record> iterateBucket(String bucketName);

  void deleteRecord(Record record);

  long countType(String typeName, boolean polymorphic);

  long countBucket(String bucketName);

  MutableDocument newDocument(String typeName);

  MutableEmbeddedDocument newEmbeddedDocument(String typeName);

  MutableVertex newVertex(String typeName);

  Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKey, Object[] sourceVertexValue, String destinationVertexType, String[] destinationVertexKey,
      Object[] destinationVertexValue, boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties);

  Edge newEdgeByKeys(Vertex sourceVertex, String destinationVertexType, String[] destinationVertexKey, Object[] destinationVertexValue,
      boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties);

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
