/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 16/07/16.
 */
public interface AggregationContext {


  Object getFinalValue();

  void apply(Result next, CommandContext ctx);
}
