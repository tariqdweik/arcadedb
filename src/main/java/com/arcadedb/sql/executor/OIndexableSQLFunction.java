package com.arcadedb.sql.executor;

import com.arcadedb.database.PRecord;
import com.arcadedb.sql.parser.OBinaryCompareOperator;
import com.arcadedb.sql.parser.OExpression;
import com.arcadedb.sql.parser.OFromClause;

public interface OIndexableSQLFunction {
  boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object right, OCommandContext context,
      OExpression[] oExpressions);

  boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object right, OCommandContext context,
      OExpression[] oExpressions);

  boolean canExecuteInline(OFromClause target, OBinaryCompareOperator operator, Object right, OCommandContext context,
      OExpression[] oExpressions);

  long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression[] oExpressions);

  Iterable<PRecord> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression[] oExpressions);
}
