package com.arcadedb.engine;

public class PWALException extends RuntimeException {
  public PWALException(final String s) {
    super(s);
  }

  public PWALException(String s, Exception e) {
    super(s, e);
  }
}
