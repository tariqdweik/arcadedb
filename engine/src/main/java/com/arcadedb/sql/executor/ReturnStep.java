/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.SimpleExecStatement;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class ReturnStep extends AbstractExecutionStep {
  private final SimpleExecStatement statement;
  boolean executed = false;

  public ReturnStep(SimpleExecStatement statement, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.statement = statement;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (executed) {
      return new InternalResultSet();
    }
    executed = true;
    return statement.executeSimple(ctx);
  }
}
