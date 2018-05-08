package com.arcadedb.exception;

import java.io.IOException;

public class SchemaException extends RuntimeException {
  public SchemaException(final String s) {
    super(s);
  }

  public SchemaException(String s, IOException e) {
    super(s, e);
  }
}
