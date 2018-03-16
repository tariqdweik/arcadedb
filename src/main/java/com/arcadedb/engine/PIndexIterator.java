package com.arcadedb.engine;

import java.io.IOException;

public interface PIndexIterator {
  Object[] getKeys();

  Object getValue();

  boolean hasNext() throws IOException;

  void next() throws IOException;

  void close();
}
