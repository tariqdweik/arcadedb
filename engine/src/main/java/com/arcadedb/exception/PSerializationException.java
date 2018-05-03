package com.arcadedb.exception;

import java.io.IOException;

public class PSerializationException extends RuntimeException {
  public PSerializationException(final String s) {
    super(s);
  }

  public PSerializationException(String s, IOException e) {
    super(s, e);
  }
}
