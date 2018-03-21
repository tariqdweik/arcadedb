package com.arcadedb.exception;

public class PTimeoutException extends RuntimeException {
  public PTimeoutException() {
  }

  public PTimeoutException(String message) {
    super(message);
  }

  public PTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public PTimeoutException(Throwable cause) {
    super(cause);
  }

  public PTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
