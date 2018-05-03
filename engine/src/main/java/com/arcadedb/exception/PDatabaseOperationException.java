package com.arcadedb.exception;

public class PDatabaseOperationException extends RuntimeException {
  public PDatabaseOperationException(final String s) {
    super(s);
  }

  public PDatabaseOperationException(String s, Exception e) {
    super(s, e);
  }
}
