package com.arcadedb.engine;

import java.io.IOException;

public class PWALException extends RuntimeException {
  public PWALException(final String s) {
    super(s);
  }

  public PWALException(String s, IOException e) {
    super(s, e);
  }
}
