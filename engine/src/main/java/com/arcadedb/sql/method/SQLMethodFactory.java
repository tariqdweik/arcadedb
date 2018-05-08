/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.executor.SQLMethod;

import java.util.Set;

/**
 * 
 * @author Johann Sorel (Geomatys)
 */
public interface SQLMethodFactory {

  boolean hasMethod(String iName);

  /**
   * @return Set of supported method names of this factory
   */
  Set<String> getMethodNames();

  /**
   * Create method for the given name. returned method may be a new instance each time or a constant.
   * 
   * @param name
   * @return OSQLMethod : created method
   * @throws CommandExecutionException
   *           : when method creation fail
   */
  SQLMethod createMethod(String name) throws CommandExecutionException;

}
