/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class ArcadeDBException extends RuntimeException {
  public ArcadeDBException() {
  }

  public ArcadeDBException(String message) {
    super(message);
  }

  public ArcadeDBException(String message, Throwable cause) {
    super(message, cause);
  }

  public ArcadeDBException(Throwable cause) {
    super(cause);
  }

  public ArcadeDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
