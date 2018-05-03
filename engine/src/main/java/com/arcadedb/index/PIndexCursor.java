package com.arcadedb.index;

import java.io.IOException;

public interface PIndexCursor {
  Object[] getKeys();

  Object getValue();

  boolean hasNext() throws IOException;

  void next() throws IOException;

  void close();
}
