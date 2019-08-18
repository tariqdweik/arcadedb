/*
 * Copyright (c) 2019 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class InvalidDatabaseInstanceException extends RuntimeException {
  public InvalidDatabaseInstanceException(final String s) {
    super(s);
  }

  public InvalidDatabaseInstanceException(String s, Throwable e) {
    super(s, e);
  }
}
