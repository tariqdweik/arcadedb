/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import java.io.IOException;

public interface IndexCursor {
  Object[] getKeys();

  Object getValue();

  boolean hasNext() throws IOException;

  void next() throws IOException;

  void close();
}
