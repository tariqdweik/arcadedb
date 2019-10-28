/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

public interface TrackableContent {
  int[] getModifiedRange();

  void updateModifiedRange(int start, int end);
}
