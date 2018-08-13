/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface Index {
  String getName();

  void compact() throws IOException;

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

  void put(Object[] keys, RID rid);

  void put(Object[] keys, RID rid, boolean checkForUnique);

  /**
   * Removes the keys from the index.
   *
   * @param keys
   */
  void remove(Object[] keys);

  /**
   * Removes an entry keys/rid entry from the index.
   */
  void remove(Object[] keys, RID rid);

  Map<String, Long> getStats();

  int getFileId();

  boolean isUnique();
}
