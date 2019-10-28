/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

public class LockException extends RuntimeException {

  public LockException(final Exception exception) {
    super(exception);
  }

  public LockException(final String iMessage) {
    super(iMessage);
  }

  public LockException(final String iMessage, final Exception exception) {
    super(iMessage, exception);
  }
}
