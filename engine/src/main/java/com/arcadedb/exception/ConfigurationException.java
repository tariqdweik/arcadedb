/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

public class ConfigurationException extends RuntimeException {
  public ConfigurationException(final String s) {
    super(s);
  }

  public ConfigurationException(String s, Exception e) {
    super(s, e);
  }
}
