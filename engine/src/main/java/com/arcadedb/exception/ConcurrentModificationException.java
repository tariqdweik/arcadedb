/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class ConcurrentModificationException extends NeedRetryException {
  public ConcurrentModificationException(final String s) {
    super(s);
  }
}
