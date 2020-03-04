/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer.graph;

/**
 * Interface for callback when an edge has been linked.
 */
public interface EdgeLinkedCallback {
  void onLinked(long linked);
}
