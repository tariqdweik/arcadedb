/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public interface RecordCallback {
  boolean onRecord(Record record);
}
