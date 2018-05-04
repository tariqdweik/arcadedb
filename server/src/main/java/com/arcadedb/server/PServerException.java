package com.arcadedb.server;

public class PServerException extends RuntimeException {
  public PServerException() {
  }

  public PServerException(String message) {
    super(message);
  }

  public PServerException(String message, Throwable cause) {
    super(message, cause);
  }

  public PServerException(Throwable cause) {
    super(cause);
  }

  public PServerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
