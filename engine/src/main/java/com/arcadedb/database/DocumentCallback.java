/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public interface DocumentCallback {
  boolean onRecord(Document record);
}
