package com.arcadedb.exception;

import java.io.IOException;

public class PConcurrentModificationException extends RuntimeException {
  public PConcurrentModificationException(final String s) {
    super(s);
  }

  public PConcurrentModificationException(String s, IOException e) {
    super(s, e);
  }
}
