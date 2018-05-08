package com.arcadedb.utility;

/**
 * Wake up at every variable found.
 */
public interface VariableParserListener {
  Object resolve(String iVariable);
}
