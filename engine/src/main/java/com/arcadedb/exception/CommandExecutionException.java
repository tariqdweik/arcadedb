/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class CommandExecutionException extends RuntimeException {
  public CommandExecutionException() {
  }

  public CommandExecutionException(String message) {
    super(message);
  }

  public CommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CommandExecutionException(Throwable cause) {
    super(cause);
  }

  public CommandExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
