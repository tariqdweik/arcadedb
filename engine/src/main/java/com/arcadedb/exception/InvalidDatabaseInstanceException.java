/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class InvalidDatabaseInstanceException extends ArcadeDBException {
  public InvalidDatabaseInstanceException(final String s) {
    super(s);
  }

  public InvalidDatabaseInstanceException(String s, Throwable e) {
    super(s, e);
  }
}
