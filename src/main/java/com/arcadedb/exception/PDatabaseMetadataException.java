package com.arcadedb.exception;

import java.io.IOException;

public class PDatabaseMetadataException extends RuntimeException {
  public PDatabaseMetadataException(final String s) {
    super(s);
  }

  public PDatabaseMetadataException(String s, IOException e) {
    super(s, e);
  }
}
