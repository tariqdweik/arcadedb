package com.arcadedb.sql.parser;

import com.arcadedb.sql.executor.ExecutionPlan;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class ExplainResultSet implements ResultSet {
  private final ExecutionPlan executionPlan;
  boolean hasNext = true;

  public ExplainResultSet(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }

  @Override public boolean hasNext() {
    return hasNext;
  }

  @Override public Result next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }
    ResultInternal result = new ResultInternal();
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.toResult()));
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlanAsString", x.prettyPrint(0, 3)));
    hasNext = false;
    return result;
  }

  @Override public void close() {

  }

  @Override public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
