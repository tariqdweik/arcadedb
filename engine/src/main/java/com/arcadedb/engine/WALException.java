/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.engine;

import com.arcadedb.exception.ArcadeDBException;

public class WALException extends ArcadeDBException {
  public WALException(final String s) {
    super(s);
  }

  public WALException(String s, Exception e) {
    super(s, e);
  }
}
