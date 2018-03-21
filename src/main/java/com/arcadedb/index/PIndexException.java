package com.arcadedb.index;

import java.io.IOException;

public class PIndexException extends RuntimeException {
  public PIndexException(final String s) {
    super(s);
  }

  public PIndexException(String s, IOException e) {
    super(s, e);
  }
}
