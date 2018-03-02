package com.arcadedb.database;

import com.arcadedb.engine.PFileManager;
import com.arcadedb.engine.PPageManager;
import com.arcadedb.schema.PSchema;
import com.arcadedb.serializer.PBinarySerializer;

import java.util.concurrent.Callable;

public interface PDatabase {
  void checkTransactionIsActive();

  interface PTransaction {
    void execute(PDatabase database);
  }

  String getDatabasePath();

  PTransactionContext getTransaction();

  void drop();

  void close();

  boolean isTransactionActive();

  void transaction(PTransaction txBlock);

  void setAutoTransaction(boolean autoTransaction);

  void begin();

  void commit();

  void rollback();

  void scanBucket(String bucketName, PRecordCallback callback);

  PRecord lookupByRID(PRID rid);

  void saveRecord(PModifiableDocument record);

  void saveRecord(PRecord record, String bucketName);

  void deleteRecord(PRID rid);

  int countBucket(String bucketName);

  PModifiableDocument newDocument();

  PVertex newVertex();

  PEdge newEdge();

  PSchema getSchema();

  PFileManager getFileManager();

  PRecordFactory getRecordFactory();

  PBinarySerializer getSerializer();

  PPageManager getPageManager();

  Object executeInLock(Callable<Object> callable);
}
