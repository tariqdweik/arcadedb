package com.arcadedb.exception;

import java.io.IOException;

public class PConfigurationException extends RuntimeException {
  public PConfigurationException(final String s) {
    super(s);
  }

  public PConfigurationException(String s, IOException e) {
    super(s, e);
  }
}
