/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

public interface RecordInternal {
  void setIdentity(RID rid);

  Binary getBuffer();

  void setBuffer(Binary array);

  void unsetDirty();
}
