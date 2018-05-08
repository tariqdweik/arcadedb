/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class ConcurrentModificationException extends RuntimeException {
  public ConcurrentModificationException(final String s) {
    super(s);
  }

  public ConcurrentModificationException(String s, IOException e) {
    super(s, e);
  }
}
