/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
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
