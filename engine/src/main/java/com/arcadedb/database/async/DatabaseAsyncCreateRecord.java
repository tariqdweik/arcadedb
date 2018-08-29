/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;

public class DatabaseAsyncCreateRecord extends DatabaseAsyncAbstractTask {
  public final Record record;
  public final Bucket bucket;

  public DatabaseAsyncCreateRecord(final Record record, final Bucket bucket) {
    this.record = record;
    this.bucket = bucket;
  }

  @Override
  public String toString() {
    return "CreateRecord(" + record + ")";
  }
}
