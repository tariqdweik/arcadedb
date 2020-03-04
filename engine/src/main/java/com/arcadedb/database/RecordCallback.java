/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

public interface RecordCallback {
  boolean onRecord(Record record);
}
