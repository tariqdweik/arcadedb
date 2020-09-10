/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class NeedRetryException extends ArcadeDBException {
  public NeedRetryException(final String s) {
    super(s);
  }

  public NeedRetryException(String s, Throwable e) {
    super(s, e);
  }
}
