/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.io.IOException;

public class ConfigurationException extends RuntimeException {
  public ConfigurationException(final String s) {
    super(s);
  }

  public ConfigurationException(String s, IOException e) {
    super(s, e);
  }
}
