package com.arcadedb.exception;

public class PQueryParsingException extends RuntimeException {
  public PQueryParsingException() {
  }

  public PQueryParsingException(String message) {
    super(message);
  }

  public PQueryParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public PQueryParsingException(Throwable cause) {
    super(cause);
  }

  public PQueryParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
