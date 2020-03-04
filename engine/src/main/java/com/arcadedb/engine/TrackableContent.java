/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.engine;

public interface TrackableContent {
  int[] getModifiedRange();

  void updateModifiedRange(int start, int end);
}
