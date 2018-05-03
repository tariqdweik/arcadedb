package com.arcadedb.exception;

public class PCommandSQLParsingException extends RuntimeException {
  public PCommandSQLParsingException() {
  }

  public PCommandSQLParsingException(String message) {
    super(message);
  }

  public PCommandSQLParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public PCommandSQLParsingException(Throwable cause) {
    super(cause);
  }

  public PCommandSQLParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
