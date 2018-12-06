/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer.graph;

/**
 * Interface for callback when an edge has been linked.
 */
public interface EdgeLinkedCallback {
  void onLinked(long linked);
}
