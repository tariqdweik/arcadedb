package com.arcadedb.exception;

import java.io.IOException;

public class PDuplicatedKeyException extends RuntimeException {
  public PDuplicatedKeyException(final String s) {
    super(s);
  }

  public PDuplicatedKeyException(String s, IOException e) {
    super(s, e);
  }
}
