/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class DatabaseIsClosedException extends RuntimeException {
  public DatabaseIsClosedException(final String s) {
    super(s);
  }

  public DatabaseIsClosedException(String s, IOException e) {
    super(s, e);
  }
}
