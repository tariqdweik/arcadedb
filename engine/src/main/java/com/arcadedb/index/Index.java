/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.io.IOException;

public interface Index extends ReadOnlyIndex {
  /**
   * Add a key in the index. If the index is UNIQUE and the key is already present, a {@link com.arcadedb.exception.DuplicatedKeyException} exception is thrown.
   *
   * @param keys
   * @param rid
   */
  void put(Object[] keys, RID rid);

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

  boolean compact() throws IOException, InterruptedException;

  boolean scheduleCompaction();
}
