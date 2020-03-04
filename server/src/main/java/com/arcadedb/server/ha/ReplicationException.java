/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
