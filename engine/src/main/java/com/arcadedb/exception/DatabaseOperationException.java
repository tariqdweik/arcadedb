/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class DatabaseOperationException extends RuntimeException {
  public DatabaseOperationException(final String s) {
    super(s);
  }

  public DatabaseOperationException(String s, Exception e) {
    super(s, e);
  }
}
