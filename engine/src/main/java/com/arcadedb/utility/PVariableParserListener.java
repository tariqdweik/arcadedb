package com.arcadedb.utility;

/**
 * Wake up at every variable found.
 */
public interface PVariableParserListener {
  Object resolve(String iVariable);
}
