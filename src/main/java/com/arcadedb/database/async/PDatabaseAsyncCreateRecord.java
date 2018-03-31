package com.arcadedb.database.async;

import com.arcadedb.database.PRecord;
import com.arcadedb.engine.PBucket;

public class PDatabaseAsyncCreateRecord extends PDatabaseAsyncCommand {
  public final PRecord record;
  public final PBucket bucket;

  public PDatabaseAsyncCreateRecord(final PRecord record, final PBucket bucket, final POkCallback onResultCallback,
      final PErrorCallback onErrorCallback) {
    super(onResultCallback, onErrorCallback);
    this.record = record;
    this.bucket = bucket;
  }
}
