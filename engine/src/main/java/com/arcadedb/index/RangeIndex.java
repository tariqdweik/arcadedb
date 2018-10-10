/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

/**
 * Basic Range Index interface. Supports range queries and iterations.
 */
public interface RangeIndex extends Index {
  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(Object[] fromKeys, boolean inclusive);

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(boolean ascendingOrder);

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(boolean ascendingOrder, Object[] fromKeys, boolean inclusive);

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor range(Object[] beginKeys, boolean beginKeysInclusive, Object[] endKeys, boolean endKeysInclusive);
}
