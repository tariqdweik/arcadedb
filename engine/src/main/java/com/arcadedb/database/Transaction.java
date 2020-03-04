/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.WALFile;

public interface Transaction {
  void begin();

  Binary commit();

  void setUseWAL(boolean useWAL);

  void setWALFlush(WALFile.FLUSH_TYPE flush);

  void rollback();

  boolean isActive();

  boolean isAsyncFlush();

  void setAsyncFlush(boolean value);
}
