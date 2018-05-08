/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

public class WALException extends RuntimeException {
  public WALException(final String s) {
    super(s);
  }

  public WALException(String s, Exception e) {
    super(s, e);
  }
}
