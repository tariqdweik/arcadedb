/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.AndBlock;
import com.arcadedb.sql.parser.BinaryCondition;

/**
 * For internal use.
 * It is used to keep info about an index range search,
 * where the main condition has the lower bound and the additional condition has the upper bound on last field only
 */
class IndexCondPair {

  AndBlock        mainCondition;
  BinaryCondition additionalRange;

  public IndexCondPair(AndBlock keyCondition, BinaryCondition additionalRangeCondition) {
    this.mainCondition = keyCondition;
    this.additionalRange = additionalRangeCondition;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    IndexCondPair that = (IndexCondPair) o;

    if (mainCondition != null ? !mainCondition.equals(that.mainCondition) : that.mainCondition != null)
      return false;
    return additionalRange != null ? additionalRange.equals(that.additionalRange) : that.additionalRange == null;
  }

  @Override public int hashCode() {
    int result = mainCondition != null ? mainCondition.hashCode() : 0;
    result = 31 * result + (additionalRange != null ? additionalRange.hashCode() : 0);
    return result;
  }
}
