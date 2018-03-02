package com.arcadedb.exception;

import java.io.IOException;

public class PDatabaseIsClosedException extends RuntimeException {
  public PDatabaseIsClosedException(final String s) {
    super(s);
  }

  public PDatabaseIsClosedException(String s, IOException e) {
    super(s, e);
  }
}
