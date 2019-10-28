/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface InternalExecutionPlan extends ExecutionPlan {

  String JAVA_TYPE = "javaType";

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the execution contains less than N
   * elements, then the result will contain them all, next result(s) will contain zero elements
   *
   * @param n
   *
   * @return
   */
  ResultSet fetchNext(int n);

  void reset(CommandContext ctx);

  long getCost();

  default Result serialize() {
    throw new UnsupportedOperationException();
  }

  default void deserialize(Result serializedExecutionPlan) {
    throw new UnsupportedOperationException();
  }

  default InternalExecutionPlan copy(CommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  boolean canBeCached();
}
