/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

/**
 * Wake up at every variable found.
 */
public interface VariableParserListener {
  Object resolve(String iVariable);
}
