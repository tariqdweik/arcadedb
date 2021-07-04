/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadetrader.com)
 */

package com.arcadedb.postgresw;

import com.arcadedb.exception.ArcadeDBException;

public class PostgresProtocolException extends ArcadeDBException {
  public PostgresProtocolException() {
  }

  public PostgresProtocolException(String message) {
    super(message);
  }

  public PostgresProtocolException(String message, Throwable cause) {
    super(message, cause);
  }

  public PostgresProtocolException(Throwable cause) {
    super(cause);
  }
}
