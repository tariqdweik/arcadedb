package com.arcadedb.database;

import com.arcadedb.engine.PBucket;
import com.arcadedb.graph.PGraphEngine;
import com.arcadedb.schema.PDocumentType;

public interface PDatabaseInternal extends PDatabase {
  PGraphEngine getGraphEngine();

  void createRecord(PModifiableDocument record);

  void createRecord(PRecord record, String bucketName);

  void createRecordNoLock(PRecord record, String bucketName);

  void updateRecord(PRecord record);

  void indexDocument(PModifiableDocument record, PDocumentType type, PBucket bucket);

}
