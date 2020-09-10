/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class DatabaseMetadataException extends ArcadeDBException {
  public DatabaseMetadataException(final String s) {
    super(s);
  }

  public DatabaseMetadataException(String s, Exception e) {
    super(s, e);
  }
}
