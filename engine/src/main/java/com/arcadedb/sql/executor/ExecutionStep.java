/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 20/07/16.
 */
public interface ExecutionStep {

  String getName();

  String getType();

  String getTargetNode();

  String getDescription();

  List<ExecutionStep> getSubSteps();

  /**
   * returns the absolute cost (in nanoseconds) of the execution of this step
   *
   * @return the absolute cost (in nanoseconds) of the execution of this step, -1 if not calculated
   */
  default long getCost() {
    return -1l;
  }

  default Result toResult() {
    ResultInternal result = new ResultInternal();
    result.setProperty("name", getName());
    result.setProperty("type", getType());
    result.setProperty("targetNode", getType());
    result.setProperty(InternalExecutionPlan.JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("subSteps",
        getSubSteps() == null ? null : getSubSteps().stream().map(x -> x.toResult()).collect(Collectors.toList()));
    result.setProperty("description", getDescription());
    return result;
  }

}
