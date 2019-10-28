/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function;

/**
 * Abstract class to extend to build Custom SQL Functions that saves the configured parameters. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * 
 */
public abstract class SQLFunctionConfigurableAbstract extends SQLFunctionAbstract {
  protected Object[] configuredParameters;

  protected SQLFunctionConfigurableAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
    configuredParameters = iConfiguredParameters;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(name);
    buffer.append('(');
    if (configuredParameters != null) {
      for (int i = 0; i < configuredParameters.length; ++i) {
        if (i > 0)
          buffer.append(',');
        buffer.append(configuredParameters[i]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }
}
