/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.utility;

import com.arcadedb.exception.ArcadeDBException;

public class LockException extends ArcadeDBException {

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
