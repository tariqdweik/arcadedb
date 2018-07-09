/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class NeedRetryException extends RuntimeException {
  public NeedRetryException(final String s) {
    super(s);
  }

  public NeedRetryException(String s, Throwable e) {
    super(s, e);
  }
}
