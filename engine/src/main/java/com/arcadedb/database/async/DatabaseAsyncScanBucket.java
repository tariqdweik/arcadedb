/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;

import java.util.concurrent.CountDownLatch;

public class DatabaseAsyncScanBucket extends DatabaseAsyncAbstractTask {
  public final CountDownLatch   semaphore;
  public final DocumentCallback userCallback;
  public final Bucket           bucket;

  public DatabaseAsyncScanBucket(final CountDownLatch semaphore, final DocumentCallback userCallback, final Bucket bucket) {
    this.semaphore = semaphore;
    this.userCallback = userCallback;
    this.bucket = bucket;
  }

  @Override
  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    try {
      bucket.scan((rid, view) -> {
        if (async.isShutdown())
          return false;

        final Record record = database.getRecordFactory()
            .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);

        return userCallback.onRecord((Document) record);
      });
    } finally {
      // UNLOCK THE CALLER THREAD
      semaphore.countDown();
    }
  }

  @Override
  public String toString() {
    return "ScanBucket(" + bucket + ")";
  }
}
