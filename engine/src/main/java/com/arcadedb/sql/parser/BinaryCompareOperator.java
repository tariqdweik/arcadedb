/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.parser;

/**
 * Created by luigidellaquila on 12/11/14.
 */
public interface BinaryCompareOperator {
  public boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();

  BinaryCompareOperator copy();

  default boolean isRangeOperator(){
    return false;
  }
}
