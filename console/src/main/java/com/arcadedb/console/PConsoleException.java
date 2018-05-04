package com.arcadedb.console;

public class PConsoleException extends RuntimeException {
  public PConsoleException() {
  }

  public PConsoleException(String message) {
    super(message);
  }

  public PConsoleException(String message, Throwable cause) {
    super(message, cause);
  }

  public PConsoleException(Throwable cause) {
    super(cause);
  }

  public PConsoleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
