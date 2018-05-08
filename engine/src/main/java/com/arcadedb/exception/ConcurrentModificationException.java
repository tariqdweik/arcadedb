package com.arcadedb.exception;

import java.io.IOException;

public class ConcurrentModificationException extends RuntimeException {
  public ConcurrentModificationException(final String s) {
    super(s);
  }

  public ConcurrentModificationException(String s, IOException e) {
    super(s, e);
  }
}
