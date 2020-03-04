/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

public interface DocumentCallback {
  boolean onRecord(Document record);
}
