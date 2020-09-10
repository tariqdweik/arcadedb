/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

import com.arcadedb.database.RID;

public class DuplicatedKeyException extends ArcadeDBException {
  private String indexName;
  private String keys;
  private RID    currentIndexedRID;

  public DuplicatedKeyException(final String indexName, final String keys, final RID currentIndexedRID) {
    super("Duplicated key " + keys + " found on index '" + indexName + "' already assigned to record " + currentIndexedRID);
    this.indexName = indexName;
    this.keys = keys;
    this.currentIndexedRID = currentIndexedRID;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getKeys() {
    return keys;
  }

  public RID getCurrentIndexedRID() {
    return currentIndexedRID;
  }
}
