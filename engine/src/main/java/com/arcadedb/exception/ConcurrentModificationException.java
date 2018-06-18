/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class ConcurrentModificationException extends NeedRetryException {
  public ConcurrentModificationException(final String s) {
    super(s);
  }

  public ConcurrentModificationException(String s, Exception e) {
    super(s, e);
  }
}
