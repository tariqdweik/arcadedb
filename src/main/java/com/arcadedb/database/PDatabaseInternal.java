package com.arcadedb.database;

import com.arcadedb.engine.PBucket;
import com.arcadedb.graph.PGraphEngine;
import com.arcadedb.schema.PDocumentType;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Internal API, do not use as an end user.
 */
public interface PDatabaseInternal extends PDatabase {
  enum CALLBACK_EVENT {
    TX_LAST_OP, TX_AFTER_WAL_WRITE, DB_NOT_CLOSED
  }

  void registerCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void unregisterCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void executeCallbacks(CALLBACK_EVENT event) throws IOException;

  PGraphEngine getGraphEngine();

  PTransactionManager getTransactionManager();

  void createRecord(PModifiableDocument record);

  void createRecord(PRecord record, String bucketName);

  void createRecordNoLock(PRecord record, String bucketName);

  void updateRecord(PRecord record);

  void updateRecordNoLock(PRecord record);

  void indexDocument(PModifiableDocument record, PDocumentType type, PBucket bucket);

  void kill();
}
