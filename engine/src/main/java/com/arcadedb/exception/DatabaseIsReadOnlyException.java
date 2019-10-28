/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class DatabaseIsReadOnlyException extends RuntimeException {
  public DatabaseIsReadOnlyException(final String s) {
    super(s);
  }

  public DatabaseIsReadOnlyException(String s, IOException e) {
    super(s, e);
  }
}
