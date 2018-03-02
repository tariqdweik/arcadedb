package com.arcadedb.exception;

public class PTransactionException extends RuntimeException {
  public PTransactionException(final String s) {
    super(s);
  }

  public PTransactionException(String s, Exception e) {
    super(s, e);
  }
}
