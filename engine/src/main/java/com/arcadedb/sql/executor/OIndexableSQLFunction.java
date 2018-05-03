package com.arcadedb.sql.executor;

import com.arcadedb.database.PRecord;
import com.arcadedb.sql.parser.BinaryCompareOperator;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.FromClause;

public interface OIndexableSQLFunction {
  boolean shouldExecuteAfterSearch(FromClause target, BinaryCompareOperator operator, Object right, OCommandContext context,
      Expression[] oExpressions);

  boolean allowsIndexedExecution(FromClause target, BinaryCompareOperator operator, Object right, OCommandContext context,
      Expression[] oExpressions);

  boolean canExecuteInline(FromClause target, BinaryCompareOperator operator, Object right, OCommandContext context,
      Expression[] oExpressions);

  long estimate(FromClause target, BinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      Expression[] oExpressions);

  Iterable<PRecord> searchFromTarget(FromClause target, BinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      Expression[] oExpressions);
}
