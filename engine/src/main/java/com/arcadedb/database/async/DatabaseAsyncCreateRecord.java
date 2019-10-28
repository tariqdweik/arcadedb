/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncCreateRecord extends DatabaseAsyncAbstractTask {
  public final Record            record;
  public final Bucket            bucket;
  public final NewRecordCallback callback;

  public DatabaseAsyncCreateRecord(final Record record, final Bucket bucket, final NewRecordCallback callback) {
    this.record = record;
    this.bucket = bucket;
    this.callback = callback;
  }

  @Override
  public void execute(DatabaseAsyncExecutor.AsyncThread async, DatabaseInternal database) {
    try {
      database.createRecordNoLock(record, bucket.getName());

      if (record instanceof MutableDocument) {
        final MutableDocument doc = (MutableDocument) record;
        database.getIndexer().createDocument(doc, database.getSchema().getType(doc.getType()), bucket);
      }

      if (callback != null)
        callback.call(record);

    } catch (Exception e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Error on executing async create operation (threadId=%d)", e, Thread.currentThread().getId());

      async.onError(e);
    }
  }

  @Override
  public String toString() {
    return "CreateRecord(" + record + ")";
  }
}
