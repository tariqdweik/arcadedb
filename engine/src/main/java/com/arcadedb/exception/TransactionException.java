/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class TransactionException extends ArcadeDBException {
  public TransactionException(final String s) {
    super(s);
  }

  public TransactionException(String s, Throwable e) {
    super(s, e);
  }
}
