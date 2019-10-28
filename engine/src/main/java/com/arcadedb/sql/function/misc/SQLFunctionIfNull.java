/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

/**
 * Returns the passed <code>field/value</code> (or optional parameter <code>return_value_if_not_null</code>) if
 * <code>field/value</code> is <b>not</b> null; otherwise it returns <code>return_value_if_null</code>.
 * 
 * <p>
 * Syntax: <blockquote>
 * 
 * <pre>
 * ifnull(&lt;field|value&gt;, &lt;return_value_if_null&gt; [,&lt;return_value_if_not_null&gt;])
 * </pre>
 * 
 * </blockquote>
 * 
 * <p>
 * Examples: <blockquote>
 * 
 * <pre>
 * SELECT <b>ifnull('a', 'b')</b> FROM ...
 *  -> 'a'
 * 
 * SELECT <b>ifnull('a', 'b', 'c')</b> FROM ...
 *  -> 'c'
 * 
 * SELECT <b>ifnull(null, 'b')</b> FROM ...
 *  -> 'b'
 * 
 * SELECT <b>ifnull(null, 'b', 'c')</b> FROM ...
 *  -> 'b'
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Mark Bigler
 */

public class SQLFunctionIfNull extends SQLFunctionAbstract {

  public static final String NAME = "ifnull";

  public SQLFunctionIfNull() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute( Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {
    /*
     * iFuncParams [0] field/value to check for null [1] return value if [0] is null [2] optional return value if [0] is not null
     */
    if (iParams[0] != null) {
      if (iParams.length == 3) {
        return iParams[2];
      }
      return iParams[0];
    }
    return iParams[1];
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifnull(<field|value>, <return_value_if_null> [,<return_value_if_not_null>])";
  }
}
