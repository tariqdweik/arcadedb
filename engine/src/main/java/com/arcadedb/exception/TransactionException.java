/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class TransactionException extends RuntimeException {
  public TransactionException(final String s) {
    super(s);
  }

  public TransactionException(String s, Throwable e) {
    super(s, e);
  }
}
