/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.executor;

import com.arcadedb.database.DatabaseInternal;

import java.util.Map;

/**
 * Basic interface for commands. Manages the context variables during execution.
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * 
 */
public interface CommandContext {
  enum TIMEOUT_STRATEGY {
    RETURN, EXCEPTION
  }

  Object getVariable(String iName);

  Object getVariable(String iName, Object iDefaultValue);

  CommandContext setVariable(String iName, Object iValue);

  CommandContext incrementVariable(String getNeighbors);

  Map<String, Object> getVariables();

  CommandContext getParent();

  CommandContext setParent(CommandContext iParentContext);

  CommandContext setChild(CommandContext context);

  /**
   * Updates a counter. Used to record metrics.
   * 
   * @param iName
   *          Metric's name
   * @param iValue
   *          delta to add or subtract
   * @return
   */
  long updateMetric(String iName, long iValue);

  boolean isRecordingMetrics();

  CommandContext setRecordingMetrics(boolean recordMetrics);

  void beginExecution(long timeoutMs, TIMEOUT_STRATEGY iStrategy);

  /**
   * Check if timeout is elapsed, if defined.
   * 
   * @return false if it the timeout is elapsed and strategy is "return"
   *              if the strategy is "exception" (default)
   */
  boolean checkTimeout();

  Map<Object, Object> getInputParameters();

  void setInputParameters(Map<Object, Object> inputParameters);

  /**
   * Creates a copy of execution context.
   */
  CommandContext copy();

  /**
   * Merges a context with current one.
   * 
   * @param iContext
   */
  void merge(CommandContext iContext);

  DatabaseInternal getDatabase();

}
