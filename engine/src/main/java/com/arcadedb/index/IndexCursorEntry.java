/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Identifiable;

import java.util.Arrays;
import java.util.Objects;

public class IndexCursorEntry {
  public final Object[]     keys;
  public final Identifiable record;
  public final int          score;

  public IndexCursorEntry(final Object[] keys, final Identifiable record, final int score) {
    this.keys = keys;
    this.record = record;
    this.score = score;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final IndexCursorEntry that = (IndexCursorEntry) o;
    return score == that.score && Objects.equals(record, that.record) && Arrays.equals(keys, that.keys);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(record, score);
    result = 31 * result + Arrays.hashCode(keys);
    return result;
  }
}
