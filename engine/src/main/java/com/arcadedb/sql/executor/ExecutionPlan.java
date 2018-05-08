/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import java.io.Serializable;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface ExecutionPlan extends Serializable{

  List<ExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  Result toResult();

}
