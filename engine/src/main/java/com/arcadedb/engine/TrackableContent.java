package com.arcadedb.engine;

public interface TrackableContent {
  int[] getModifiedRange();

  void updateModifiedRange(int start, int end);
}
