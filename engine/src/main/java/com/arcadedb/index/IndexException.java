/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import java.io.IOException;

public class IndexException extends RuntimeException {
  public IndexException(final String s) {
    super(s);
  }

  public IndexException(String s, IOException e) {
    super(s, e);
  }
}
