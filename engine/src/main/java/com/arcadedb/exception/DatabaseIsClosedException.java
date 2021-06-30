/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class DatabaseIsClosedException extends ArcadeDBException {
  public DatabaseIsClosedException(final String s) {
    super(s);
  }

  public DatabaseIsClosedException(String s, IOException e) {
    super(s, e);
  }
}
