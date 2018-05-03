package com.arcadedb.exception;

public class PCommandExecutionException extends RuntimeException {
  public PCommandExecutionException() {
  }

  public PCommandExecutionException(String message) {
    super(message);
  }

  public PCommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public PCommandExecutionException(Throwable cause) {
    super(cause);
  }

  public PCommandExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
