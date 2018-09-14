/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

public class IndexCursorEntry {
  public final Object[] keys;
  public final RID      rid;
  public final int      score;

  public IndexCursorEntry(final Object[] keys, final RID rid, final int score) {
    this.keys = keys;
    this.rid = rid;
    this.score = score;
  }
}
