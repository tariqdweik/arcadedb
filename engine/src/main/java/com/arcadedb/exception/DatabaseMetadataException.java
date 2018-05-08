/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class DatabaseMetadataException extends RuntimeException {
  public DatabaseMetadataException(final String s) {
    super(s);
  }

  public DatabaseMetadataException(String s, Exception e) {
    super(s, e);
  }
}
