/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

public class QueryParsingException extends RuntimeException {
  public QueryParsingException() {
  }

  public QueryParsingException(String message) {
    super(message);
  }

  public QueryParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryParsingException(Throwable cause) {
    super(cause);
  }

  public QueryParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
