package com.arcadedb.exception;

public class CommandSQLParsingException extends RuntimeException {
  public CommandSQLParsingException() {
  }

  public CommandSQLParsingException(String message) {
    super(message);
  }

  public CommandSQLParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public CommandSQLParsingException(Throwable cause) {
    super(cause);
  }

  public CommandSQLParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
