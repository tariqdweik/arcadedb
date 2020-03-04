/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
