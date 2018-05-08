package com.arcadedb.exception;

public class TransactionException extends RuntimeException {
  public TransactionException(final String s) {
    super(s);
  }

  public TransactionException(String s, Exception e) {
    super(s, e);
  }
}
