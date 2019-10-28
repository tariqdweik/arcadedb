/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.parser;

import com.arcadedb.database.DatabaseInternal;

/**
 * Created by luigidellaquila on 12/11/14.
 */
public interface BinaryCompareOperator {
  boolean execute(DatabaseInternal database, Object left, Object right);

  boolean supportsBasicCalculation();

  BinaryCompareOperator copy();

  default boolean isRangeOperator(){
    return false;
  }
}
