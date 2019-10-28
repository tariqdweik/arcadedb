/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

public class ReplicationException extends RuntimeException {
  public ReplicationException(final String message) {
    super(message);
  }

  public ReplicationException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
