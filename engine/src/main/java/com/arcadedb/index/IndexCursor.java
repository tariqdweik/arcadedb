/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

public interface IndexCursor {
  Object[] getKeys();

  Object getValue();

  boolean hasNext();

  Object next();

  void close();
}
