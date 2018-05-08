/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Abstract class for multi-value based function implementations.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class SQLFunctionMultiValueAbstract<T> extends SQLFunctionConfigurableAbstract {

  protected T context;

  public SQLFunctionMultiValueAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  @Override
  public T getResult() {
    return context;
  }
}
