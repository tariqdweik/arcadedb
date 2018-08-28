/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface ReadOnlyIndex {
  void close();

  String getName();

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(Object[] fromKeys) throws IOException;

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(boolean ascendingOrder) throws IOException;

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor iterator(boolean ascendingOrder, Object[] fromKeys) throws IOException;

  /**
   * The returning iterator does not skip deleted entries.
   */
  IndexCursor range(Object[] beginKeys, Object[] endKeys) throws IOException;

  Set<RID> get(Object[] keys);

  Set<RID> get(Object[] keys, final int limit);

  Map<String, Long> getStats();

  int getFileId();

  boolean isUnique();

  PaginatedComponent getPaginatedComponent();
}
