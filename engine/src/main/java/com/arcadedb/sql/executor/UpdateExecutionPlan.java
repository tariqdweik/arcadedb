/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 08/08/16.
 */

import com.arcadedb.exception.CommandExecutionException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UpdateExecutionPlan extends SelectExecutionPlan {

  List<Result> result = new ArrayList<>();
  int          next   = 0;

  public UpdateExecutionPlan(CommandContext ctx) {
    super(ctx);
  }

  @Override
  public ResultSet fetchNext(int n) {
    if (next >= result.size()) {
      return new InternalResultSet();//empty
    }

    IteratorResultSet nextBlock = new IteratorResultSet(result.subList(next, Math.min(next + n, result.size())).iterator());
    next += n;
    return nextBlock;
  }

  @Override
  public void reset(CommandContext ctx) {
    result.clear();
    next = 0;
    super.reset(ctx);
    executeInternal();
  }

  public void executeInternal() throws CommandExecutionException {
    while (true) {
      ResultSet nextBlock = super.fetchNext(100);
      if (!nextBlock.hasNext()) {
        return;
      }
      while (nextBlock.hasNext()) {
        result.add(nextBlock.next());
      }
    }
  }

  @Override
  public Result toResult() {
    ResultInternal res = (ResultInternal) super.toResult();
    res.setProperty("type", "UpdateExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}

