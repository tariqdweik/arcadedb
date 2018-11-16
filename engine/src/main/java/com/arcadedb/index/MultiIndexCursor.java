/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiIndexCursor implements IndexCursor {
  private final List<IndexCursor> cursors;
  private final int               limit;
  private       int               browsed      = 0;
  private       int               currentIndex = 0;
  private       IndexCursor       current;

  public MultiIndexCursor(final List<IndexCursor> cursors, final int limit) {
    this.cursors = cursors;
    this.limit = limit;
  }

  public MultiIndexCursor(final List<RangeIndex> indexes, final boolean ascendingOrder, final int limit) {
    this.cursors = new ArrayList<>(indexes.size());
    this.limit = limit;
    for (RangeIndex i : indexes)
      this.cursors.add(i.iterator(ascendingOrder));
  }

  @Override
  public Object[] getKeys() {
    return getCurrent().getKeys();
  }

  @Override
  public RID getRID() {
    return getCurrent().getRID();
  }

  @Override
  public boolean hasNext() {
    if (limit > -1 && browsed > limit) {
      current = null;
      return false;
    }

    while (current != null) {
      if (current.hasNext())
        return true;

      if (currentIndex < cursors.size() - 1)
        current = cursors.get(+currentIndex);
      else
        current = null;
    }

    return false;
  }

  @Override
  public RID next() {
    if (hasNext()) {
      ++browsed;
      return current.next();
    }

    throw new NoSuchElementException();
  }

  @Override
  public int getScore() {
    return getCurrent().getScore();
  }

  @Override
  public void close() {
    for (IndexCursor cursor : cursors)
      cursor.close();
  }

  @Override
  public String dumpStats() {
    return "no-stats";
  }

  @Override
  public int size() {
    int tot = 0;
    for (IndexCursor cursor : cursors)
      tot += cursor.size();
    return tot;
  }

  @Override
  public Iterator<RID> iterator() {
    return this;
  }

  private IndexCursor getCurrent() {
    if (current == null)
      throw new NoSuchElementException();

    return current;
  }
}
