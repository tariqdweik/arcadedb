/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.database.*;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.*;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.sql.parser.ExecutionPlanCache;
import com.arcadedb.sql.parser.StatementCache;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

public class ServerDatabaseProxy implements DatabaseInternal {
  private final DatabaseInternal proxied;
  private       boolean          open = true;

  public ServerDatabaseProxy(final DatabaseInternal proxied) {
    this.proxied = proxied;
  }

  public DatabaseInternal getProxied() {
    return proxied;
  }

  @Override
  public void close() {
    open = false;
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("Server proxied database instance cannot be drop");
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    checkForOpen();
    proxied.registerCallback(event, callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    checkForOpen();
    proxied.unregisterCallback(event, callback);
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    checkForOpen();
    proxied.executeCallbacks(event);
  }

  @Override
  public GraphEngine getGraphEngine() {
    checkForOpen();
    return proxied.getGraphEngine();
  }

  @Override
  public TransactionManager getTransactionManager() {
    checkForOpen();
    return proxied.getTransactionManager();
  }

  @Override
  public void createRecord(final ModifiableDocument record) {
    checkForOpen();
    proxied.createRecord(record);
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    checkForOpen();
    proxied.createRecord(record, bucketName);
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName) {
    checkForOpen();
    proxied.createRecordNoLock(record, bucketName);
  }

  @Override
  public void updateRecord(final Record record) {
    checkForOpen();
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record) {
    checkForOpen();
    proxied.updateRecordNoLock(record);
  }

  @Override
  public void indexDocument(final ModifiableDocument record, final DocumentType type, final Bucket bucket) {
    checkForOpen();
    proxied.indexDocument(record, type, bucket);
  }

  @Override
  public void kill() {
    checkForOpen();
    proxied.kill();
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    checkForOpen();
    return proxied.getWALFileFactory();
  }

  @Override
  public StatementCache getStatementCache() {
    checkForOpen();
    return proxied.getStatementCache();
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    checkForOpen();
    return proxied.getExecutionPlanCache();
  }

  @Override
  public boolean isOpen() {
    return proxied.isOpen();
  }

  @Override
  public String getName() {
    checkForOpen();
    return proxied.getName();
  }

  @Override
  public DatabaseAsyncExecutor asynch() {
    checkForOpen();
    return proxied.asynch();
  }

  @Override
  public String getDatabasePath() {
    checkForOpen();
    return proxied.getDatabasePath();
  }

  @Override
  public TransactionContext getTransaction() {
    checkForOpen();
    return proxied.getTransaction();
  }

  @Override
  public boolean isTransactionActive() {
    checkForOpen();
    return proxied.isTransactionActive();
  }

  @Override
  public boolean checkTransactionIsActive() {
    checkForOpen();
    return proxied.checkTransactionIsActive();
  }

  @Override
  public void transaction(final Transaction txBlock) {
    checkForOpen();
    proxied.transaction(txBlock);
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    checkForOpen();
    proxied.setAutoTransaction(autoTransaction);
  }

  @Override
  public void begin() {
    checkForOpen();
    proxied.begin();
  }

  @Override
  public void commit() {
    checkForOpen();
    proxied.commit();
  }

  @Override
  public void rollback() {
    checkForOpen();
    proxied.rollback();
  }

  @Override
  public void scanType(final String className, final boolean polymorphic, final DocumentCallback callback) {
    checkForOpen();
    proxied.scanType(className, polymorphic, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    checkForOpen();
    proxied.scanBucket(bucketName, callback);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    checkForOpen();
    return proxied.lookupByRID(rid, loadContent);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    checkForOpen();
    return proxied.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    checkForOpen();
    return proxied.iterateBucket(bucketName);
  }

  @Override
  public Cursor<RID> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    checkForOpen();
    return proxied.lookupByKey(type, properties, keys);
  }

  @Override
  public void deleteRecord(final Record record) {
    checkForOpen();
    proxied.deleteRecord(record);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    checkForOpen();
    return proxied.countType(typeName, polymorphic);
  }

  @Override
  public long countBucket(final String bucketName) {
    checkForOpen();
    return proxied.countBucket(bucketName);
  }

  @Override
  public ModifiableDocument newDocument(final String typeName) {
    checkForOpen();
    return proxied.newDocument(typeName);
  }

  @Override
  public ModifiableVertex newVertex(String typeName) {
    checkForOpen();
    return proxied.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue,
      final String destinationVertexType, final String[] destinationVertexKey, final Object[] destinationVertexValue,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {
    checkForOpen();
    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKey, sourceVertexValue, destinationVertexType, destinationVertexKey,
        destinationVertexValue, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Schema getSchema() {
    checkForOpen();
    return proxied.getSchema();
  }

  @Override
  public FileManager getFileManager() {
    checkForOpen();
    return proxied.getFileManager();
  }

  @Override
  public void transaction(final Transaction txBlock, final int retries) {
    checkForOpen();
    proxied.transaction(txBlock, retries);
  }

  @Override
  public RecordFactory getRecordFactory() {
    checkForOpen();
    return proxied.getRecordFactory();
  }

  @Override
  public BinarySerializer getSerializer() {
    checkForOpen();
    return proxied.getSerializer();
  }

  @Override
  public PageManager getPageManager() {
    checkForOpen();
    return proxied.getPageManager();
  }

  @Override
  public ResultSet sql(final String query, final Map<String, Object> args) {
    checkForOpen();
    return proxied.sql(query, args);
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> args) {
    checkForOpen();
    return proxied.query(query, args);
  }

  @Override
  public Object executeInReadLock(final Callable<Object> callable) {
    checkForOpen();
    return proxied.executeInReadLock(callable);
  }

  @Override
  public Object executeInWriteLock(final Callable<Object> callable) {
    checkForOpen();
    return proxied.executeInWriteLock(callable);
  }

  @Override
  public boolean isReadYourWrites() {
    checkForOpen();
    return proxied.isReadYourWrites();
  }

  @Override
  public void setReadYourWrites(final boolean value) {
    checkForOpen();
    proxied.setReadYourWrites(value);
  }

  protected void checkForOpen() {
    if (!open)
      throw new DatabaseOperationException("Database instance '" + getName() + "' is closed");
  }
}
