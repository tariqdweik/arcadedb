/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class DatabaseOperationException extends RuntimeException {
  public DatabaseOperationException(final String s) {
    super(s);
  }

  public DatabaseOperationException(String s, Throwable e) {
    super(s, e);
  }
}
