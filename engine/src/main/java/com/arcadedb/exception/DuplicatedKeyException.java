/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class DuplicatedKeyException extends RuntimeException {
  private String indexName;
  private String keys;

  public DuplicatedKeyException(final String indexName, final String keys) {
    super("Duplicated key " + keys + " found on index '" + indexName + "'");
    this.indexName = indexName;
    this.keys = keys;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getKeys() {
    return keys;
  }
}
