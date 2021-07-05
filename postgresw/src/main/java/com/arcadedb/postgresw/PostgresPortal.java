/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.postgresw;

import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.parser.Statement;

import java.util.List;
import java.util.Map;

public class PostgresPortal {
  public String                    query;
  public List<Long>                parameterTypes;
  public List<Integer>             parameterFormats;
  public List<Object>              parameterValues;
  public List<Integer>             resultFormats;
  public Statement                 statement;
  public boolean                   ignoreExecution   = false;
  public List<Result>              cachedResultset;
  public Map<String, PostgresType> columns;
  public boolean                   isExpectingResult = true;
  public boolean                   executed          = false;

  public PostgresPortal(final String query) {
    this.query = query;
    //final String queryUpperCase = query.toUpperCase();
    this.isExpectingResult = true;//queryUpperCase.startsWith("SELECT") || queryUpperCase.startsWith("MATCH");
  }

  @Override
  public String toString() {
    return query;
  }
}
