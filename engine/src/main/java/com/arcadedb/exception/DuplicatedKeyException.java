package com.arcadedb.exception;

import java.io.IOException;

public class DuplicatedKeyException extends RuntimeException {
  public DuplicatedKeyException(final String s) {
    super(s);
  }

  public DuplicatedKeyException(String s, IOException e) {
    super(s, e);
  }
}
