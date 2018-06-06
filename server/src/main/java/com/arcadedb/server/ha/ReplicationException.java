/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

public class ReplicationException extends RuntimeException {
  public ReplicationException(final String message) {
    super(message);
  }

  public ReplicationException(final String message, final Exception cause) {
    super(message, cause);
  }
}
