/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.sql.parser.BinaryCompareOperator;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.FromClause;

public interface OIndexableSQLFunction {
  boolean shouldExecuteAfterSearch(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  boolean allowsIndexedExecution(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  boolean canExecuteInline(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  long estimate(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext ctx,
      Expression[] oExpressions);

  Iterable<Record> searchFromTarget(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext ctx,
      Expression[] oExpressions);
}
