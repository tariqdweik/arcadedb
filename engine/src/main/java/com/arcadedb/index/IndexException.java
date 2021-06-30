/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.index;

import com.arcadedb.exception.ArcadeDBException;

import java.io.IOException;

public class IndexException extends ArcadeDBException {
  public IndexException(final String s) {
    super(s);
  }

  public IndexException(String s, IOException e) {
    super(s, e);
  }
}
