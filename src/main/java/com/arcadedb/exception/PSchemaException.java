package com.arcadedb.exception;

import java.io.IOException;

public class PSchemaException extends RuntimeException {
  public PSchemaException(final String s) {
    super(s);
  }

  public PSchemaException(String s, IOException e) {
    super(s, e);
  }
}
