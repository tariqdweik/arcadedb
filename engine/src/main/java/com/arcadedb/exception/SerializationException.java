/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class SerializationException extends RuntimeException {
  public SerializationException(final String s) {
    super(s);
  }

  public SerializationException(String s, IOException e) {
    super(s, e);
  }
}
