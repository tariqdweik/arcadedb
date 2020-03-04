/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.util.logging.Level;

/**
 * Returns different values based on the condition. If it's true the first value is returned, otherwise the second one.
 * <p>
 * <p>
 * Syntax: <blockquote>
 *
 * <pre>
 * if(&lt;field|value|expression&gt;, &lt;return_value_if_true&gt; [,&lt;return_value_if_false&gt;])
 * </pre>
 *
 * </blockquote>
 * <p>
 * <p>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>if(rich, 'rich', 'poor')</b> FROM ...
 * <br>
 * SELECT <b>if( eval( 'salary > 1000000' ), 'rich', 'poor')</b> FROM ...
 * </pre>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */

public class SQLFunctionIf extends SQLFunctionAbstract {

  public static final String NAME = "if";

  public SQLFunctionIf() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute(Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {

    boolean result;

    try {
      Object condition = iParams[0];
      if (condition instanceof Boolean)
        result = (Boolean) condition;
      else if (condition instanceof String)
        result = Boolean.parseBoolean(condition.toString());
      else if (condition instanceof Number)
        result = ((Number) condition).intValue() > 0;
      else
        return null;

      return result ? iParams[1] : iParams[2];

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error during if execution", e);

      return null;
    }
  }

  @Override
  public String getSyntax() {
    return "if(<field|value|expression>, <return_value_if_true> [,<return_value_if_false>])";
  }
}
