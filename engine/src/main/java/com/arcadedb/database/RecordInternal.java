/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public interface RecordInternal {
  void setIdentity(RID rid);

  Binary getBuffer();

  void setBuffer(Binary array);

  void unsetDirty();
}
