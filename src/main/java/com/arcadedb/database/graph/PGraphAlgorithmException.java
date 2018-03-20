package com.arcadedb.database.graph;

import java.io.IOException;

public class PGraphAlgorithmException extends RuntimeException {
  public PGraphAlgorithmException(final String s) {
    super(s);
  }

  public PGraphAlgorithmException(String s, IOException e) {
    super(s, e);
  }
}
