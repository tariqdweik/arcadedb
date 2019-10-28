/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

public class ServerCommandException extends RuntimeException {
  public ServerCommandException() {
  }

  public ServerCommandException(String message) {
    super(message);
  }

  public ServerCommandException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServerCommandException(Throwable cause) {
    super(cause);
  }

  public ServerCommandException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
