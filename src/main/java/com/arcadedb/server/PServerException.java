package com.arcadedb.server;

public class PServerException extends RuntimeException {
  public PServerException(final String s) {
    super(s);
  }

  public PServerException(String s, Exception e) {
    super(s, e);
  }
}
