/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

/**
 * Returns the first <code>field/value</code> not null parameter. if no <code>field/value</code> is <b>not</b> null, returns null.
 * <p>
 * <p>
 * Syntax: <blockquote>
 *
 * <pre>
 * coalesce(&lt;field|value&gt;[,&lt;field|value&gt;]*)
 * </pre>
 *
 * </blockquote>
 * <p>
 * <p>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>coalesce('a', 'b')</b> FROM ...
 *  -> 'a'
 *
 * SELECT <b>coalesce(null, 'b')</b> FROM ...
 *  -> 'b'
 *
 * SELECT <b>coalesce(null, null, 'c')</b> FROM ...
 *  -> 'c'
 *
 * SELECT <b>coalesce(null, null)</b> FROM ...
 *  -> null
 *
 * </pre>
 *
 * </blockquote>
 *
 * @author Claudio Tesoriero
 */

public class SQLFunctionCoalesce extends SQLFunctionAbstract {
  public static final String NAME = "coalesce";

  public SQLFunctionCoalesce() {
    super(NAME, 1, 1000);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    int length = iParams.length;
    for (int i = 0; i < length; i++) {
      if (iParams[i] != null)
        return iParams[i];
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "Returns the first not-null parameter or null if all parameters are null. Syntax: coalesce(<field|value> [,<field|value>]*)";
  }
}
