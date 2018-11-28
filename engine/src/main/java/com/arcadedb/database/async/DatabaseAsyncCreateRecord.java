/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;

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
  public String toString() {
    return "CreateRecord(" + record + ")";
  }
}
