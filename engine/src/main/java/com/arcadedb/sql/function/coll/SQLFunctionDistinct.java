/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Keeps items only once removing duplicates
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionDistinct extends SQLFunctionAbstract {
  public static final String NAME = "distinct";

  private Set<Object> context = new LinkedHashSet<Object>();

  public SQLFunctionDistinct() {
    super(NAME, 1, 1);
  }

  public Object execute( Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    final Object value = iParams[0];

    if (value != null && !context.contains(value)) {
      context.add(value);
      return value;
    }

    return null;
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  public String getSyntax() {
    return "distinct(<field>)";
  }
}
