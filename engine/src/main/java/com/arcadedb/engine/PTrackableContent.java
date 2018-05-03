package com.arcadedb.engine;

public interface PTrackableContent {
  int[] getModifiedRange();

  void updateModifiedRange(int start, int end);
}
