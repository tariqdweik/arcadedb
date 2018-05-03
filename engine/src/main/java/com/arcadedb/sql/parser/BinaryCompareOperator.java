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
