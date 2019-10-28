/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.executor.SQLFunction;

import java.util.Set;

/**
 * @author Johann Sorel (Geomatys)
 */
public interface SQLFunctionFactory {

  boolean hasFunction(String iName);

  /**
   * @return Set of supported function names of this factory
   */
  Set<String> getFunctionNames();

  /**
   * Create function for the given name. returned function may be a new instance each time or a constant.
   *
   * @param name
   *
   * @return OSQLFunction : created function
   *
   * @throws CommandExecutionException : when function creation fail
   */
  SQLFunction createFunction(String name) throws CommandExecutionException;

}
