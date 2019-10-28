/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.sql.parser.ExecutionPlanCache;
import com.arcadedb.sql.parser.StatementCache;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Internal API, do not use as an end user.
 */
public interface DatabaseInternal extends Database {
  enum CALLBACK_EVENT {
    TX_AFTER_WAL_WRITE, DB_NOT_CLOSED
  }

  Map<String, Object> getStats();

  DatabaseInternal getEmbedded();

  DatabaseContext.DatabaseContextTL getContext();

  FileManager getFileManager();

  RecordFactory getRecordFactory();

  BinarySerializer getSerializer();

  PageManager getPageManager();

  DatabaseInternal getWrappedDatabaseInstance();

  void registerCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void unregisterCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void executeCallbacks(CALLBACK_EVENT event) throws IOException;

  GraphEngine getGraphEngine();

  TransactionManager getTransactionManager();

  void createRecord(MutableDocument record);

  void createRecord(Record record, String bucketName);

  void createRecordNoLock(Record record, String bucketName);

  void updateRecord(Record record);

  void updateRecordNoLock(Record record);

  void kill();

  DocumentIndexer getIndexer();

  WALFileFactory getWALFileFactory();

  StatementCache getStatementCache();

  ExecutionPlanCache getExecutionPlanCache();
}
