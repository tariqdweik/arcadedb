/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import java.util.*;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class InternalResultSet implements ResultSet {
  private   List<Result>  content = new ArrayList<>();
  private   int           next    = 0;
  protected ExecutionPlan plan;

  @Override
  public boolean hasNext() {
    return content.size() > next;
  }

  @Override
  public Result next() {
    return content.get(next++);
  }

  @Override
  public void close() {
    this.content.clear();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.ofNullable(plan);
  }

  public void setPlan(ExecutionPlan plan) {
    this.plan = plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }

  public void add(Result nextResult) {
    content.add(nextResult);
  }

  public void reset() {
    this.next = 0;
  }
}
