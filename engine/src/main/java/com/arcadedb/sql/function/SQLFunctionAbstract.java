/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function;

import com.arcadedb.sql.executor.SQLFunction;

/**
 * Abstract class to extend to build Custom SQL Functions. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class SQLFunctionAbstract implements SQLFunction {
  protected String name;
  protected int    minParams;
  protected int    maxParams;

  public SQLFunctionAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    this.name = iName;
    this.minParams = iMinParams;
    this.maxParams = iMaxParams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getMinParams() {
    return minParams;
  }

  @Override
  public int getMaxParams() {
    return maxParams;
  }

  @Override
  public String toString() {
    return name + "()";
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public boolean filterResult() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void setResult(final Object iResult) {
  }
}
