package com.arcadedb.server;

public class PServerCommandException extends RuntimeException {
  public PServerCommandException() {
  }

  public PServerCommandException(String message) {
    super(message);
  }

  public PServerCommandException(String message, Throwable cause) {
    super(message, cause);
  }

  public PServerCommandException(Throwable cause) {
    super(cause);
  }

  public PServerCommandException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
