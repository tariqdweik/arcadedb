package com.arcadedb.exception;

public class PDatabaseMetadataException extends RuntimeException {
  public PDatabaseMetadataException(final String s) {
    super(s);
  }

  public PDatabaseMetadataException(String s, Exception e) {
    super(s, e);
  }
}
