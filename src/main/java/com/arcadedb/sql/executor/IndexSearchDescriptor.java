package com.arcadedb.sql.executor;

import com.arcadedb.index.PIndex;
import com.arcadedb.sql.parser.OAndBlock;
import com.arcadedb.sql.parser.OBinaryCompareOperator;
import com.arcadedb.sql.parser.OBinaryCondition;
import com.arcadedb.sql.parser.OBooleanExpression;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class IndexSearchDescriptor {
  protected PIndex             idx;
  protected OAndBlock          keyCondition;
  protected OBinaryCondition   additionalRangeCondition;
  protected OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(PIndex idx, OAndBlock keyCondition, OBinaryCondition additional,
      OBooleanExpression remainingCondition) {
    this.idx = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {

  }

  public int cost(OCommandContext ctx) {
    OQueryStats stats = OQueryStats.get(ctx.getDatabase());

    String indexName = idx.getName();
    int size = keyCondition.getSubBlocks().size();
    boolean range = false;
    OBooleanExpression lastOp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (lastOp instanceof OBinaryCondition) {
      OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
      range = op.isRangeOperator();
    }

    long val = stats.getIndexStats(indexName, size, range, additionalRangeCondition != null);
    if (val >= 0) {
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }
    return Integer.MAX_VALUE;
  }
}
